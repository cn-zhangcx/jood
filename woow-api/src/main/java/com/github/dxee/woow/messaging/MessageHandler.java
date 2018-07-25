package com.github.dxee.woow.messaging;

import com.github.dxee.woow.WoowContext;
import com.google.protobuf.Message;


public interface MessageHandler<T extends Message> {

    /**
     * Callback interface to hand over a message.
     * <p>
     * Implementors need to consider that we have at least once deliery,
     * i.e. messages may be delivered multiple times (and potentially out of order).
     * Thus, message handlers need to handle duplicate messages gracefully / be idempotent.
     *
     * @param message
     * @param context
     */
    void onMessage(EventMessage<T> message, WoowContext context);

}
