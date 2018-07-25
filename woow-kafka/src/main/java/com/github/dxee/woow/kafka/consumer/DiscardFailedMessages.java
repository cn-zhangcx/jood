package com.github.dxee.woow.kafka.consumer;

import com.github.dxee.woow.messaging.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discard any messages that caused the MessageHandler to throw an exception.
 * <p>
 * It logs topic and offset of the message, so a out-of-bounds mechanism can process / re-try any failed messages.
 */
public class DiscardFailedMessages implements FailedMessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DiscardFailedMessages.class);

    @Override
    public boolean onFailedMessage(EventMessage failed, Throwable failureCause) {
        logger.warn("Discarded failing message.", failureCause);

        return false;
    }
}
