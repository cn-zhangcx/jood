package com.github.dxee.joo.eventhandling;

import com.google.protobuf.Message;

/**
 * A Strategy interface to allow exchangeable failure handling behaviour.
 *
 * @author bing.fan
 * 2018-07-05 17:29
 */
public interface ErrorHandler<T extends Message> {

    /**
     * Specify a strategy to manage event handler failed
     * @param eventMessage       the event message
     * @param eventHandler       the event handler
     * @param failureCause       the root cause of the failure
     */
    void handleError(EventMessage eventMessage, EventHandler<T> eventHandler, Throwable failureCause);

}
