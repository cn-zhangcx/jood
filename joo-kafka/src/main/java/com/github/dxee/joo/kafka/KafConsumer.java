package com.github.dxee.joo.kafka;

import com.github.dxee.dject.lifecycle.LifecycleListener;
import com.github.dxee.joo.eventhandling.ErrorHandler;
import com.github.dxee.joo.eventhandling.EventListener;
import com.github.dxee.joo.eventhandling.EventProcessor;
import com.github.dxee.joo.kafka.consumer.AssignedPartitions;
import com.github.dxee.joo.kafka.consumer.PartitionProcessor;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * KafConsumer instances are Kafka clients that fetch records of (one or multiple assignedPartitions of) a topic.
 * <p>
 * Consumers also handle the flow control (pausing/resuming busy assignedPartitions) as well as changes in the
 * partition assignment.
 * <p>
 * The topic a consumer subscribes to must have been created before. When starting a consumer, it will
 * check Kafka if the topic exists.
 * <p>
 * Threading model:
 * The KafConsumer has a single thread polling Kafka and handing over raw records to PartitionProcessors for
 * further processing. There is one PartitionProcessor per partition.
 * A PartitionProcessor currently is single-threaded to keep the ordering guarantee on a partition.
 * <p>
 * KafConsumer instances are created by the ConsumerFactory.
 *
 * @author bing.fan
 * 2018-07-06 18:17
 */
public class KafConsumer implements EventProcessor, LifecycleListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafConsumer.class);

    private static final int HANDLER_TIMEOUT_MILLIS = 60 * 1000;
    private static final int POLL_INTERVAL_MILLIS = 300;
    // every six hours
    private static final long COMMIT_REFRESH_INTERVAL_MILLIS = 6 * 60 * 60 * 1000;

    // synchronized because put may be executed in different thread than read access
    // if synchronization is found too heavy for this, extract interface and implement
    // an immutable dictionary and another modifiable one
    private final Map<String, EventListener<? extends Message>> eventListeners
            = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Parser<Message>> eventMessageParsers
            = Collections.synchronizedMap(new HashMap<>());
    private final ExecutorService consumerLoopExecutor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean isStopped = new AtomicBoolean(false);

    private ErrorHandler errorHandler;
    private AssignedPartitions assignedPartitions;

    private final String topic;
    private final String consumerGroupId;
    private final KafkaConsumer<String, byte[]> kafkaConsumer;

    public KafConsumer(String topic, String consumerGroupId, Properties consumerProperties) {
        this.topic = topic;
        this.consumerGroupId = consumerGroupId;
        // Mandatory settings, not changeable
        consumerProperties.put("group.id", consumerGroupId);
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    }

    @Override
    public void start() {
        this.assignedPartitions = new AssignedPartitions(this);
        consumerLoopExecutor.execute(new ConsumerLoop());
    }

    @Override
    public void shutdown() {
        LOGGER.debug("Shutdown requested for consumer in group {} for topic {}", consumerGroupId, topic);

        isStopped.set(true);
        consumerLoopExecutor.shutdown();


        int timeout = 2;
        try {
            consumerLoopExecutor.awaitTermination(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("consumer loop executor stop exception catched within {} seconds", timeout, e);
        }

        assignedPartitions.stopAllProcessor();
        assignedPartitions.waitForAllEventListenersToComplete(HANDLER_TIMEOUT_MILLIS);
        kafkaConsumer.commitSync(assignedPartitions.offsetsToBeCommitted());

        kafkaConsumer.close();

        LOGGER.info("KafConsumer in group {} for topic {} was shut down.", consumerGroupId, topic);
    }

    @Override
    public void addEventListener(String typeName, EventListener<? extends Message> eventListener) {
        eventListeners.put(typeName, eventListener);
    }


    @Override
    public EventListener<? extends Message> getEventListener(String typeName) {
        return eventListeners.get(typeName);
    }

    @Override
    public void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public void addParser(String typeName, Parser<Message> parser) {
        eventMessageParsers.put(typeName, parser);
    }

    @Override
    public Parser<Message> getParser(String typeName) {
        return eventMessageParsers.get(typeName);
    }

    @Override
    public void onStarted() {
        start();
    }

    @Override
    public void onStopped(Throwable error) {
        shutdown();
    }

    class ConsumerLoop implements Runnable {
        // single thread access only
        private long nextCommitRefreshRequiredTimestamp = System.currentTimeMillis() + COMMIT_REFRESH_INTERVAL_MILLIS;

        @Override
        public void run() {
            try {
                kafkaConsumer.subscribe(Arrays.asList(topic), new PartitionAssignmentChange());
                LOGGER.info("KafConsumer in group {} subscribed to topic {}", consumerGroupId, topic);
            } catch (Exception unexpected) {
                LOGGER.error("Dead consumer in group {}: Cannot subscribe to topic {}",
                        consumerGroupId, topic, unexpected);
                return;
            }

            try {
                while (!isStopped.get()) {
                    try {
                        // Note that poll() may also execute the ConsumerRebalanceListener
                        // callbacks and may take substantially more time to return.
                        ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(POLL_INTERVAL_MILLIS);

                        assignedPartitions.enqueue(records);
                        kafkaConsumer.commitSync(assignedPartitions.offsetsToBeCommitted());

                        checkIfRefreshCommitRequired();

                        kafkaConsumer.pause(assignedPartitions.partitionsToBePaused());
                        kafkaConsumer.resume(assignedPartitions.partitionsToBeResumed());


                    } catch (Exception kafkaException) {
                        // Example for an exception seen in testing:
                        // CommitFailedException: Commit cannot be completed since the group has already rebalanced and
                        // assigned the assignedPartitions to another member.

                        // Error handling strategy: log the exception and carry on.
                        // Do not kill the consumer as nobody is there to resurrect a new one.
                        LOGGER.warn("Received exception in ConsumerLoop of KafConsumer (group=" + consumerGroupId
                                + " ,topic=" + topic + "). KafConsumer continues.", kafkaException);
                    }
                }
            } catch (Throwable unexpectedError) {
                LOGGER.error("Unexpected exception in ConsumerLoop of KafConsumer (group=" + consumerGroupId
                        + " ,topic=" + topic + "). KafConsumer now dead.", unexpectedError);

                // Try to close the connection to Kafka
                kafkaConsumer.close();

                // Since we catched Exception already in the loop, this points to a serious issue where we probably
                // cannot recover from.
                // Thus, we let the thread and the consumer die.
                // Kafka needs to rebalance the group. But we loose a consumer in this instance as nobody takes
                // care to resurrect failed consumers.
                throw unexpectedError;
            }
        }

        private void checkIfRefreshCommitRequired() {
            // Here's the issue:
            // The retention of __consumer_offsets is less than most topics itself, so we need to re-commit
            // regularly to keep the last committed offset per consumer group. This is especially an issue
            // in cases were we have bursty / little traffic.

            Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
            long now = System.currentTimeMillis();

            if (nextCommitRefreshRequiredTimestamp < now) {
                nextCommitRefreshRequiredTimestamp = now + COMMIT_REFRESH_INTERVAL_MILLIS;

                for (PartitionProcessor processor : assignedPartitions.allProcessors()) {
                    TopicPartition assignedPartition = processor.getAssignedPartition();
                    long lastCommittedOffset = processor.getLastCommittedOffset();

                    // We haven't committed from this partiton yet
                    if (lastCommittedOffset < 0) {
                        OffsetAndMetadata offset = kafkaConsumer.committed(assignedPartition);
                        if (offset == null) {
                            // there was no commit on this partition at all
                            continue;
                        }
                        lastCommittedOffset = offset.offset();
                        processor.forceSetLastCommittedOffset(lastCommittedOffset);
                    }


                    commitOffsets.put(assignedPartition, new OffsetAndMetadata(lastCommittedOffset));
                }


                kafkaConsumer.commitSync(commitOffsets);

                LOGGER.info("Refreshing last committed offset {}", commitOffsets);
            }
        }

    }

    class PartitionAssignmentChange implements ConsumerRebalanceListener {
        // From the documentation:
        // 1. This callback will execute in the user thread as part of the
        // {@link KafConsumer#poll(long) poll(long)} call whenever partition assignment changes.
        // 2. It is guaranteed that all consumer processes will invoke
        // {@link #onPartitionsRevoked(Collection) onPartitionsRevoked} prior to any process invoking
        // {@link #onPartitionsAssigned(Collection) onPartitionsAssigned}.

        // Observations from the tests:
        // 1. When Kafka rebalances assignedPartitions, all currently assigned assignedPartitions are revoked and
        // then the remaining assignedPartitions are newly assigned.
        // 2. There seems to be a race condition when Kafka is starting up in parallel
        // (e.g. service integration tests): an empty partition set is assigned and
        // we do not receive any messages.

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> revokedPartitions) {
            LOGGER.debug("ConsumerRebalanceListener.onPartitionsRevoked on {}", revokedPartitions);

            assignedPartitions.stopProcessor(revokedPartitions);
            assignedPartitions.waitForEventListenersToComplete(revokedPartitions, HANDLER_TIMEOUT_MILLIS);

            kafkaConsumer.commitSync(assignedPartitions.offsetsToBeCommitted());

            assignedPartitions.removePartitions(revokedPartitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assignedPartitions) {
            LOGGER.debug("ConsumerRebalanceListener.onPartitionsAssigned on {}", assignedPartitions);
            KafConsumer.this.assignedPartitions.assignNewPartitions(assignedPartitions);
        }
    }
}
