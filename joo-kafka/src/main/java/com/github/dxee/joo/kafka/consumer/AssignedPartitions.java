package com.github.dxee.joo.kafka.consumer;

import com.github.dxee.joo.eventhandling.ErrorHandler;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * AssignedPartitions represents the set of PartitionProcessors for the partitions assigned to a certain consumer.
 * <p>
 * Read records are dispatched to the right processor. It aggregates over the individual processors to get the set of
 * committed offset, paused or resumed partitions.
 * <p>
 * Additionally it manages creation, termination and removal of PartitionProcessors as partitions are distributed across
 * consumers.
 * <p>
 * Thread safety policy: single-thread use by KafConsumer
 *
 * @author bing.fan
 * 2018-07-06 18:09
 */
public final class AssignedPartitions {
    private static final Logger LOGGER = LoggerFactory.getLogger(AssignedPartitions.class);

    private final Map<TopicPartition, PartitionProcessor> processors = new HashMap<>();
    private final EventListeners eventListeners;
    private final ErrorHandler errorHandler;

    public AssignedPartitions(EventListeners eventListeners, ErrorHandler errorHandler) {
        this.eventListeners = eventListeners;
        this.errorHandler = errorHandler;
    }

    public Set<TopicPartition> allPartitions() {
        Set<TopicPartition> assignedPartitions = new HashSet<>();
        processors.forEach((key, partition) -> assignedPartitions.add(key));
        return assignedPartitions;
    }

    public void assignNewPartitions(Collection<TopicPartition> assignedPartitions) {
        assignedPartitions.forEach((key) -> assignNewPartition(key));
    }

    private PartitionProcessor assignNewPartition(TopicPartition partitionKey) {
        LOGGER.debug("Assigning new PartitionProcessor for partition {}", partitionKey);

        PartitionProcessor proccessor = new PartitionProcessor(partitionKey, eventListeners, errorHandler);
        processors.put(partitionKey, proccessor);

        return proccessor;
    }

    public void enqueue(ConsumerRecords<String, byte[]> records) {
        records.forEach((record) -> {
            TopicPartition partitionKey = new TopicPartition(record.topic(), record.partition());

            PartitionProcessor processor = processors.get(partitionKey);
            if (processor == null) {
                processor = assignNewPartition(partitionKey);
            }

            processor.enqueue(record);
        });
    }

    public Map<TopicPartition, OffsetAndMetadata> offsetsToBeCommitted() {
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();

        processors.forEach((key, processor) -> {
            if (processor.hasUncommittedMessages()) {
                commitOffsets.put(key, new OffsetAndMetadata(processor.getCommitOffsetAndClear()));
            }
        });

        return commitOffsets;
    }

    public Collection<TopicPartition> partitionsToBePaused() {
        List<TopicPartition> pausedPartitions = new ArrayList<>();

        processors.forEach((key, processor) -> {
            if (processor.isPaused()) {
                pausedPartitions.add(key);
            }
        });

        return pausedPartitions;
    }

    public Collection<TopicPartition> partitionsToBeResumed() {
        List<TopicPartition> resumeablePartitions = new ArrayList<>();

        processors.forEach((key, processor) -> {
            if (processor.shouldResume()) {
                resumeablePartitions.add(key);
            }
        });

        return resumeablePartitions;
    }

    public void stopProcessing(Collection<TopicPartition> partitions) {
        partitions.forEach((key) -> {
            PartitionProcessor processor = processors.get(key);

            if (processor == null) {
                LOGGER.warn("Ignored operation: trying to stop a non-existing processor for partition {}", key);
                return;
            }

            processor.stopProcessing();
        });
    }

    public void waitForEventListenersToComplete(Collection<TopicPartition> partitions, long timeoutMillis) {
        partitions.forEach((key) -> {
            PartitionProcessor processor = processors.get(key);

            if (processor == null) {
                LOGGER.warn("Ignored operation: trying to waitForEventListenersToTerminate on a "
                        + "non-existing processor for partition {}", key);
                return;
            }

            processor.waitForEventListenersToTerminate(timeoutMillis);
        });
    }

    public void removePartitions(Collection<TopicPartition> revokedPartitions) {
        revokedPartitions.forEach((key) -> {
            PartitionProcessor processor = processors.get(key);

            if (processor == null) {
                return; // idempotent
            }

            if (!processor.isTerminated()) {
                throw new IllegalStateException("Processor must be terminated before removing it.");
            }

            LOGGER.debug("Removing PartitionProcessor for partition {}", key);
            processors.remove(key);
        });
    }

    public Collection<PartitionProcessor> allProcessors() {
        Collection<PartitionProcessor> allProcessors = new ArrayList<>();

        processors.forEach((key, processor) -> {
            allProcessors.add(processor);
        });

        return allProcessors;
    }
}
