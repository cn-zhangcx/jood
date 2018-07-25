package com.github.dxee.woow.kafka.consumer;

import com.github.dxee.woow.eventhandling.EventListener;
import com.github.dxee.woow.eventhandling.MessageType;
import com.google.common.base.MoreObjects;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EventListenerMapping {

    // synchronized because put may be executed in different thread than read access
    // if synchronization is found too heavy for this, extract interface and implement
    // an immutable dictionary and another modifiable one
    private final Map<MessageType, Parser<Message>> parsers
            = Collections.synchronizedMap(new HashMap<>());
    private final Map<MessageType, EventListener<? extends Message>> handlers
            = Collections.synchronizedMap(new HashMap<>());


    public EventListenerMapping() {
    }

    /**
     * @param type
     * @return null if no EventListener was found for the type, otherwise the handler
     */
    public EventListener<? extends Message> getEventListener(MessageType type) {
        return handlers.get(type);
    }


    public EventListener<? extends Message> addListener(MessageType type, EventListener<? extends Message> handler) {
        return handlers.put(type, handler);
    }

    /**
     * @param type
     * @return null if no Parser was found for the type, otherwise the parser
     */
    public Parser<Message> getParser(MessageType type) {
        return parsers.get(type);
    }

    public Parser addParser(MessageType type, Parser<Message> parser) {
        return parsers.put(type, parser);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("parsers", parsers)
                .add("handlers", handlers)
                .toString();
    }
}
