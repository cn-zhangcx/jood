package com.github.dxee.woow.kafka.consumer;

import com.github.dxee.woow.WoowContext;
import com.github.dxee.woow.kafka.Envelope;
import com.github.dxee.woow.kafka.Messages;
import com.github.dxee.woow.kafka.UnknownMessageHandlerException;
import com.github.dxee.woow.kafka.UnknownMessageTypeException;
import com.github.dxee.woow.messaging.EventMessage;
import com.github.dxee.woow.messaging.MessageHandler;
import com.github.dxee.woow.messaging.MessageType;
import com.github.dxee.woow.messaging.TypeDictionary;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class PartitionProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionProcessor.class);

    static final int MAX_MESSAGES_IN_FLIGHT = 100;

    // The partition processor is a queue plus a worker thread.
    private final BlockingQueue<Runnable> undeliveredMessages;
    private final ThreadPoolExecutor executor;

    // Which partition is this processor responsible for?
    private final TopicPartition partitionKey;

    // Injected
    private final TypeDictionary typeDictionary;
    private final FailedMessageProcessor failedMessageProcessor;

    // Lifecycle state
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final AtomicBoolean isTerminated = new AtomicBoolean(false);

    // Offset/commit handling
    private final AtomicLong lastConsumedOffset = new AtomicLong(-2);
    private final AtomicLong lastComittedOffset = new AtomicLong(-1);

    // Lifecycle --------------------------------------------------

    PartitionProcessor(TopicPartition partitionKey, TypeDictionary typeDictionary,
                       FailedMessageProcessor failedMessageProcessor) {
        this.partitionKey = partitionKey;
        this.typeDictionary = typeDictionary;
        this.failedMessageProcessor = failedMessageProcessor;

        undeliveredMessages = new LinkedBlockingQueue<>();

        // Single threaded execution per partition to preserve ordering guarantees.
        // EXTENSION:
        // - if required, allow multiple threads sacrificing ordering.
        // - but then the commmit offset handling requires more thoughts
        executor = new ThreadPoolExecutor(1, 1, 24,
                TimeUnit.HOURS, undeliveredMessages);
    }

    void stopProcessing() {
        // We mark this dispatcher as stopped, so no new tasks will execute.
        isStopped.set(true);
        executor.shutdown();
    }

    boolean isTerminated() {
        return isTerminated.get();
    }

    void waitForHandlersToTerminate(long timeoutMillis) {
        stopProcessing(); // ensure that we're shutting down

        try {

            boolean terminatedSuccessfully = executor.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS);

            if (!terminatedSuccessfully) {
                LOGGER.warn("PartitionProcessor {}: still running message handlers after waiting {} ms to terminate.",
                        partitionKey, timeoutMillis);
            }

            isTerminated.set(true);

        } catch (InterruptedException e) {
            LOGGER.warn("PartitionProcessor {}: Interrupted while waiting to terminate.", partitionKey);
        }
    }


    // Message dispatch --------------------------------------------------
    public void enqueue(ConsumerRecord<String, byte[]> record) {
        if (isStopped.get()) {
            LOGGER.info("Ignored records to be enqueued after PartitionProcessor {} was stopped.", partitionKey);
            return;
        }

        executor.submit(new MessageDeliveryTask(record));
    }

    class MessageDeliveryTask implements Runnable {

        private final ConsumerRecord<String, byte[]> record;

        MessageDeliveryTask(ConsumerRecord<String, byte[]> record) {
            this.record = record;
        }

        @Override
        public void run() {
            if (isStopped.get()) {
                // empty the queue if the processor was stopped.
                return;
            }

            try {
                EventMessage<? extends Message> message = parseMessage();
                if (message == null) {
                    // Can not even parse the message, so we give up.
                    return;
                }

                deliverToMessageHandler(message);

            } catch (Throwable unexpectedError) {
                // Anything that reaches here could be potentially a condition that the thread could not recover from.
                // see https://docs.oracle.com/javase/specs/jls/se8/html/jls-11.html#jls-11.1
                //
                // Thus, we try to log the error, but let the thread die.
                // The thread pool will create a new thread is the hosting process itself is still alive.
                LOGGER.error("Unexpected error while handling message", unexpectedError);
                throw unexpectedError;
            }
        }


        private EventMessage<? extends Message> parseMessage() {
            Envelope envelope = null;

            try {
                envelope = Envelope.parseFrom(record.value());
            } catch (InvalidProtocolBufferException parseError) {
                markAsConsumed(record.offset());
                parsingFailed(envelope, parseError);
                return null;
            }

            try {
                MessageType type = new MessageType(envelope.getMessageType());

                Parser<Message> parser = typeDictionary.parserFor(type);
                if (parser == null) {
                    throw new UnknownMessageTypeException(type);
                }

                Message innerMessage = parser.parseFrom(envelope.getInnerMessage());
                return Messages.fromKafka(innerMessage, envelope, record);
            } catch (InvalidProtocolBufferException | UnknownMessageTypeException unrecoverableParsingError) {
                markAsConsumed(record.offset());
                parsingFailed(envelope, unrecoverableParsingError);
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        private void deliverToMessageHandler(EventMessage message) {
            boolean tryDeliverMessage = true;
            boolean deliveryFailed = true;

            WoowContext context = message.getMetadata().newContextFromMetadata();

            try {
                while (tryDeliverMessage) {
                    try {
                        MessageType messageType = message.getMetadata().getType();
                        MessageHandler handler = typeDictionary.messageHandlerFor(messageType);
                        if (handler == null) {
                            throw new UnknownMessageHandlerException(messageType);
                        }

                        deliveryStarted(message, handler, context);

                        // Leave the framework here: hand over execution to service-specific handler.
                        handler.onMessage(message, context);
                        deliveryFailed = false;

                        break;
                    } catch (Exception failure) {
                        // Strategy decides: Should we retry to deliver the failed message?
                        tryDeliverMessage = failedMessageProcessor.onFailedMessage(message, failure);
                        deliveryFailed(message, failure, tryDeliverMessage);
                    }
                }

            } finally {
                // consume the message - even if delivery failed
                markAsConsumed(message.getMetadata().getOffset());
                deliveryEnded(message, deliveryFailed);
            }


        }


        // Helper methods to get the glue code for debug logging, tracing and metrics out of the main control flow
        private void parsingFailed(Envelope envelope, Exception parseException) {
            // TODO log, metrics
            LOGGER.warn("parsingFailed {}", envelope, parseException);
        }


        private void deliveryStarted(EventMessage message, MessageHandler handler, WoowContext context) {
            // TODO log, trace, metrics
            LOGGER.debug("deliveryStarted {}, {}, {}", message, handler.getClass().getName(), context);
        }

        private void deliveryFailed(EventMessage message, Exception failure, boolean tryDeliverMessage) {
            // TODO log, metrics
            LOGGER.warn("deliveryFailed {}", message, failure);
        }

        private void deliveryEnded(EventMessage message, boolean deliveryFailed) {
            // TODO log, trace, metrics
            LOGGER.debug("deliveryEnded {}", message);
        }
    }

    // Offset / commit handling --------------------------------------------------

    TopicPartition getAssignedPartition() {
        return partitionKey;
    }


    int numberOfUnprocessedMessages() {
        // Thread safety: snapshot value
        return undeliveredMessages.size();
    }

    void markAsConsumed(long messageOffset) {
        // Single threaded execution preserves strict ordering.
        lastConsumedOffset.set(messageOffset);
    }

    boolean hasUncommittedMessages() {
        // Thread safety: it's ok to use a snapshot of the lastConsumedOffset,
        // as we will have constant progress on this value.
        // So it doesn't matter if we use a bit outdated value;
        // we would be exact if we called this method a few milliseconds before. ;-)
        return lastComittedOffset.get() < (lastConsumedOffset.get() + 1);
    }

    long getCommitOffsetAndClear() {
        // Commit offset always points to next unconsumed message.
        // Thread safety: see hasUncommittedMessages()

        lastComittedOffset.set(lastConsumedOffset.get() + 1);
        return lastComittedOffset.get();
    }

    long getLastCommittedOffset() {
        return lastComittedOffset.get();
    }

    void forceSetLastCommittedOffset(long lastComittedOffset) {
        LOGGER.info("forceSetLastCommittedOffset of partition {} to {}", partitionKey, lastComittedOffset);
        this.lastComittedOffset.set(lastComittedOffset);
    }


    // Flow control --------------------------------------------------
    boolean isPaused() {
        return numberOfUnprocessedMessages() > MAX_MESSAGES_IN_FLIGHT;
    }

    boolean shouldResume() {
        // simple logic for now - from the resume docs: "If the partitions were not previously paused,
        // this method is a no-op."
        return !isPaused();
    }

    // Test access
    TypeDictionary getTypeDictionary() {
        return typeDictionary;
    }
}
