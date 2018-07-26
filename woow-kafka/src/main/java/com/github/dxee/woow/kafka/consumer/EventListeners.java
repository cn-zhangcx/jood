package com.github.dxee.woow.kafka.consumer;

import com.github.dxee.woow.eventhandling.EventListener;
import com.google.common.base.MoreObjects;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * EventListeners of protobuf message parser and event listener.
 *
 * @author bing.fan
 * 2018-07-06 18:06
 */
public class EventListeners {
    // synchronized because put may be executed in different thread than read access
    // if synchronization is found too heavy for this, extract interface and implement
    // an immutable dictionary and another modifiable one
    private final Map<String, Parser<Message>> eventMessageParsers
            = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, EventListener<? extends Message>> eventListeners
            = Collections.synchronizedMap(new HashMap<>());

    /**
     * @param typeName
     * @return null if no EventListener was found for the typeName, otherwise the handler
     */
    public EventListener<? extends Message> getEventListener(String typeName) {
        return eventListeners.get(typeName);
    }


    public EventListener<? extends Message> addListener(String typeName,
                                                        EventListener<? extends Message> eventListener) {
        return eventListeners.put(typeName, eventListener);
    }

    /**
     * @param typeName
     * @return null if no Parser was found for the typeName, otherwise the parser
     */
    public Parser<Message> getParser(String typeName) {
        return eventMessageParsers.get(typeName);
    }

    public Parser addParser(String typeName, Parser<Message> parser) {
        return eventMessageParsers.put(typeName, parser);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("eventMessageParsers", eventMessageParsers)
                .add("eventListeners", eventListeners)
                .toString();
    }
}
