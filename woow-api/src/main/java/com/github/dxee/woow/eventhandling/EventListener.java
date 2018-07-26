package com.github.dxee.woow.eventhandling;

import com.github.dxee.woow.WoowContext;
import com.google.protobuf.Message;

/**
 * Interface to be implemented by classes that can handle events.
 *
 * @author bing.fan
 * 2018-07-06 18:37
 */
public interface EventListener<T extends Message> {

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
    void handle(EventMessage<T> eventMessage, WoowContext context);

}
