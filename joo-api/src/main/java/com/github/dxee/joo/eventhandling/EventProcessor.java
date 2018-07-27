package com.github.dxee.joo.eventhandling;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

/**
 * Event processor
 *
 * @author bing.fan
 * 2018-07-06 18:25
 */
public interface EventProcessor {

    /**
     * Add event listener to the event processor
     * @param typeName the event message type name
     * @param eventListener the event listener
     */
    void addEventListener(String typeName, EventListener<? extends Message> eventListener);

    EventListener<? extends Message> getEventListener(String typeName);

    /**
     * Add error handler to event processor
     * @param errorHandler the error handler
     */
    void setErrorHandler(ErrorHandler errorHandler);

    ErrorHandler getErrorHandler();

    /**
     * Add event messeage protobuf parser to the event processor
     * @param typeName the event listener type name
     * @param parser the event message protobuf parser
     */
    void addParser(String typeName, Parser<Message> parser);

    Parser<Message> getParser(String typeName);

    /**
     * Start processing events.
     */
    void start();

    /**
     * Stop processing events.
     */
    void shutdown();
}
