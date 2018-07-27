package com.github.dxee.joo.kafka;

import com.github.dxee.joo.eventhandling.ErrorHandler;
import com.github.dxee.joo.eventhandling.EventListener;
import com.github.dxee.joo.eventhandling.ListenerRegister;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Listeners registry
 *
 * @author bing.fan
 * 2018-07-02 14:20
 */
public class ListenersRegistry implements ListenerRegister {
    // synchronized because put may be executed in different thread than read access
    // if synchronization is found too heavy for this, extract interface and implement
    // an immutable dictionary and another modifiable one
    private final Map<String, EventListener<? extends Message>> eventListeners
            = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Parser<? extends Message>> eventMessageParsers
            = Collections.synchronizedMap(new HashMap<>());
    private final ErrorHandler errorHandler;

    public ListenersRegistry(Map<String, EventListener<? extends Message>> eventListeners,
                             Map<String, Parser<? extends Message>> eventMessageParsers,
                             ErrorHandler errorHandler) {
        this.eventListeners.putAll(eventListeners);
        this.eventMessageParsers.putAll(eventMessageParsers);
        this.errorHandler = errorHandler;
    }

    @Override
    public EventListener<? extends Message> getEventListener(String typeName) {
        return eventListeners.get(typeName);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public Parser<? extends Message> getParser(String typeName) {
        return eventMessageParsers.get(typeName);
    }
}
