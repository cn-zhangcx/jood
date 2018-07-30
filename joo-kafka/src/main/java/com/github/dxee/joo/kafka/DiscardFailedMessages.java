package com.github.dxee.joo.kafka;

import com.github.dxee.joo.eventhandling.ErrorHandler;
import com.github.dxee.joo.eventhandling.EventHandler;
import com.github.dxee.joo.eventhandling.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discard failed event message
 * @author bing.fan
 * 2018-07-11 11:42
 */
public class DiscardFailedMessages implements ErrorHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscardFailedMessages.class);

    @Override
    public void handleError(EventMessage eventMessage, EventHandler eventHandler, Throwable failureCause) {
        LOGGER.warn("Discarded failing, message {}, event handler {}", eventMessage, eventHandler, failureCause);
    }
}
