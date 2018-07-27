package com.github.dxee.joo.eventhandling;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

/**
 * Listener register
 *
 * @author bing.fan
 * 2018-07-02 14:04
 */
public interface ListenerRegister {
    EventListener<? extends Message> getEventListener(String typeName);

    ErrorHandler getErrorHandler();

    Parser<? extends Message> getParser(String typeName);
}
