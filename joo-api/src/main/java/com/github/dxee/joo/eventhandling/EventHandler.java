package com.github.dxee.joo.eventhandling;

import com.github.dxee.joo.JooContext;
import com.google.protobuf.Message;

import java.lang.reflect.InvocationTargetException;

/**
 * Interface to be implemented by classes that can handle events.
 *
 * @author bing.fan
 * 2018-07-06 18:37
 */
public interface EventHandler {

    /**
     * Callback interface to hand over a event.
     * <p>
     * Implementors need to consider that we have at least once deliery,
     * i.e. event may be delivered multiple times (and potentially out of order).
     * Thus, event listeners need to handle duplicate messages gracefully / be idempotent.
     *
     * @param eventMessage
     * @param context
     */
    void handle(EventMessage<? extends Message> eventMessage, JooContext context)
            throws InvocationTargetException, IllegalAccessException;

}
