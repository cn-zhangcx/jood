package com.github.dxee.joo.eventhandling;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.Set;

/**
 * Event handler register
 *
 * @author bing.fan
 * 2018-07-02 14:04
 */
public interface EventHandlerRegister {
    Set<EventHandler> getEventHandler(String typeName);

    ErrorHandler getErrorHandler();

    Parser<? extends Message> getParser(String typeName);
}
