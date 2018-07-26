package com.github.dxee.woow.kafka.consumer;

import com.github.dxee.woow.eventhandling.ErrorHandler;
import com.github.dxee.woow.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discard any messages that caused the EventListener to throw an exception.
 * <p>
 * It logs topic and offset of the message, so a out-of-bounds mechanism can process / re-try any failed messages.
 */
public class DiscardFailedMessages implements ErrorHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscardFailedMessages.class);

    @Override
    public boolean handleError(EventMessage eventMessage, Throwable failureCause) {
        LOGGER.warn("Discarded failing message.", failureCause);
        return false;
    }
}
