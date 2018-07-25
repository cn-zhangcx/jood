package com.github.dxee.woow.messaging;

import com.google.common.base.MoreObjects;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TypeDictionary {

    // synchronized because put may be executed in different thread than read access
    // if synchronization is found too heavy for this, extract interface and implement
    // an immutable dictionary and another modifiable one
    private final Map<MessageType, Parser<Message>> parsers
            = Collections.synchronizedMap(new HashMap<>());
    private final Map<MessageType, MessageHandler<? extends Message>> handlers
            = Collections.synchronizedMap(new HashMap<>());


    public TypeDictionary() {
    }

    public TypeDictionary(Map<MessageType, MessageHandler<? extends Message>> handlers,
                          Map<MessageType, Parser<Message>> parsers) {
        this.handlers.putAll(handlers);
        this.parsers.putAll(parsers);
    }

    /**
     * @param type
     * @return null if no MessageHandler was found for the type, otherwise the handler
     */
    public MessageHandler<? extends Message> messageHandlerFor(MessageType type) {
        return handlers.get(type);
    }

    /**
     * @param type
     * @return null if no Parser was found for the type, otherwise the parser
     */
    public Parser<Message> parserFor(MessageType type) {
        return parsers.get(type);
    }


    public MessageHandler<? extends Message> putHandler(MessageType type, MessageHandler<? extends Message> handler) {
        return handlers.put(type, handler);
    }

    public Parser putParser(MessageType type, Parser<Message> parser) {
        return parsers.put(type, parser);
    }

    public void putAllParsers(Map<MessageType, Parser<Message>> parsers) {
        this.parsers.putAll(parsers);
    }

    public void putAllHandlers(Map<MessageType, MessageHandler<? extends Message>> handlers) {
        this.handlers.putAll(handlers);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("parsers", parsers)
                .add("handlers", handlers)
                .toString();
    }
}
