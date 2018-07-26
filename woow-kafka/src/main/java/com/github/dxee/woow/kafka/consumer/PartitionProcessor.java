package com.github.dxee.woow.kafka.consumer;

import com.github.dxee.woow.WoowContext;
import com.github.dxee.woow.kafka.Envelope;
import com.github.dxee.woow.kafka.EventMessages;
import com.github.dxee.woow.kafka.UnknownMessageTypeException;
import com.github.dxee.woow.eventhandling.*;
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

public final class PartitionProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionProcessor.class);

    static final int MAX_MESSAGES_IN_FLIGHT = 100;

    // The partition processor is a queue plus a worker thread.
    private final BlockingQueue<Runnable> undeliveredMessages;
    private final ThreadPoolExecutor executor;

    // Which partition is this processor responsible for?
    private final TopicPartition partitionKey;

    // Injected
    private final EventListeners eventListeners;
    private final ErrorHandler errorHandler;

    // Lifecycle state
    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final AtomicBoolean isTerminated = new AtomicBoolean(false);

    // Offset/commit handling
    private final AtomicLong lastConsumedOffset = new AtomicLong(-2);
    private final AtomicLong lastComittedOffset = new AtomicLong(-1);

    // Lifecycle --------------------------------------------------

    PartitionProcessor(TopicPartition partitionKey, EventListeners eventListeners,
                       ErrorHandler errorHandler) {
        this.partitionKey = partitionKey;
        this.eventListeners = eventListeners;
        this.errorHandler = errorHandler;

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

    void waitForEventListenersToTerminate(long timeoutMillis) {
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
                EventMessage<? extends Message> eventMessage = parseEventMessage();
                if (eventMessage == null) {
                    // Can not even parse the eventMessage, so we give up.
                    return;
                }

                deliverToEventListener(eventMessage);

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


        private EventMessage<? extends Message> parseEventMessage() {
            Envelope envelope = null;

            try {
                envelope = Envelope.parseFrom(record.value());
            } catch (InvalidProtocolBufferException parseError) {
                markAsConsumed(record.offset());
                parsingFailed(envelope, parseError);
                return null;
            }

            try {
                String type = envelope.getTypeName();

                Parser<Message> parser = eventListeners.getParser(type);
                if (parser == null) {
                    throw new UnknownMessageTypeException(type);
                }

                Message innerMessage = parser.parseFrom(envelope.getInnerMessage());
                return EventMessages.fromKafka(innerMessage, envelope, record);
            } catch (InvalidProtocolBufferException | UnknownMessageTypeException unrecoverableParsingError) {
                markAsConsumed(record.offset());
                parsingFailed(envelope, unrecoverableParsingError);
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        private void deliverToEventListener(EventMessage eventMessage) {
            boolean tryDeliverMessage = true;
            boolean deliveryFailed = true;

            WoowContext context = eventMessage.getMetadata().newContextFromMetadata();

            try {
                while (tryDeliverMessage) {
                    try {
                        String typeName = eventMessage.getMetadata().getTypeName();
                        EventListener eventListener = eventListeners.getEventListener(typeName);
                        if (eventListener == null) {
                            throw new IllegalArgumentException(typeName);
                        }

                        deliveryStarted(eventMessage, eventListener, context);

                        // Leave the framework here: hand over execution to service-specific eventListener.
                        eventListener.handle(eventMessage, context);
                        deliveryFailed = false;

                        break;
                    } catch (Exception failure) {
                        // Strategy decides: Should we retry to deliver the failed eventMessage?
                        tryDeliverMessage = errorHandler.handleError(eventMessage, failure);
                        deliveryFailed(eventMessage, failure, tryDeliverMessage);
                    }
                }
            } finally {
                // consume the eventMessage - even if delivery failed
                markAsConsumed(eventMessage.getMetadata().getOffset());
                deliveryEnded(eventMessage, deliveryFailed);
            }


        }


        // Helper methods to get the glue code for debug logging, tracing and metrics out of the main control flow
        private void parsingFailed(Envelope envelope, Exception parseException) {
            // TODO log, metrics
            LOGGER.warn("parsingFailed {}", envelope, parseException);
        }


        private void deliveryStarted(EventMessage message, EventListener handler, WoowContext context) {
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

    public TopicPartition getAssignedPartition() {
        return partitionKey;
    }


    private int numberOfUnprocessedMessages() {
        // Thread safety: snapshot value
        return undeliveredMessages.size();
    }

    private void markAsConsumed(long messageOffset) {
        // Single threaded execution preserves strict ordering.
        lastConsumedOffset.set(messageOffset);
    }

    protected boolean hasUncommittedMessages() {
        // Thread safety: it's ok to use a snapshot of the lastConsumedOffset,
        // as we will have constant progress on this value.
        // So it doesn't matter if we use a bit outdated value;
        // we would be exact if we called this method a few milliseconds before. ;-)
        return lastComittedOffset.get() < (lastConsumedOffset.get() + 1);
    }

    protected long getCommitOffsetAndClear() {
        // Commit offset always points to next unconsumed message.
        // Thread safety: see hasUncommittedMessages()

        lastComittedOffset.set(lastConsumedOffset.get() + 1);
        return lastComittedOffset.get();
    }

    public long getLastCommittedOffset() {
        return lastComittedOffset.get();
    }

    public void forceSetLastCommittedOffset(long lastComittedOffset) {
        LOGGER.info("forceSetLastCommittedOffset of partition {} to {}", partitionKey, lastComittedOffset);
        this.lastComittedOffset.set(lastComittedOffset);
    }


    // Flow control --------------------------------------------------
    protected boolean isPaused() {
        return numberOfUnprocessedMessages() > MAX_MESSAGES_IN_FLIGHT;
    }

    protected boolean shouldResume() {
        // simple logic for now - from the resume docs: "If the partitions were not previously paused,
        // this method is a no-op."
        return !isPaused();
    }
}
