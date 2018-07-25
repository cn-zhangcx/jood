package com.github.dxee.woow.kafka.consumer;

import com.github.dxee.woow.messaging.EventMessage;

// Thread safety: single thread use
public class DelayAndRetryOnRecoverableErrors implements FailedMessageProcessor {

    private final FailedMessageProcessor fallbackStrategy;
    private final RetryDelayer retryStrategy;

    EventMessage lastFailedMessage = null;

    public DelayAndRetryOnRecoverableErrors(FailedMessageProcessor fallbackStrategy, RetryDelayer retryStrategy) {
        this.fallbackStrategy = fallbackStrategy;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public boolean onFailedMessage(EventMessage failed, Throwable failureCause) {
        if (!isRecoverable(failureCause)) {
            return fallbackStrategy.onFailedMessage(failed, failureCause);
        }

        // we have a new failure case
        if (failed != lastFailedMessage) { // reference equals by intention
            lastFailedMessage = failed;
            retryStrategy.reset();
        }

        // blocks the message handler thread -> flow control may pause the partition
        boolean shouldRetry = retryStrategy.delay();

        if (!shouldRetry) {
            return fallbackStrategy.onFailedMessage(failed, failureCause);
        }

        return shouldRetry;
    }

    /**
     * This method can be overridden to specify custom behaviour.
     * <p>
     * The default implementation simply returns false (non-retryable) in all cases.
     *
     * @param failureCause The exception thrown when delivering the message.
     * @return true if the message delivery should be retried, false otherwise.
     */
    protected boolean isRecoverable(Throwable failureCause) {
        return false;
    }


}
