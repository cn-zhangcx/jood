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
     *  Get the listener register of event processor
     * @return
     */
    ListenerRegister getListenerRegister();

    /**
     * Start processing events.
     */
    void start();

    /**
     * Stop processing events.
     */
    void shutdown();
}
