package com.github.dxee.woow.kafka;


import com.github.dxee.woow.messaging.MessageType;

public class UnknownMessageHandlerException extends Exception {
    public UnknownMessageHandlerException(MessageType type) {
        super(type.toString());
    }
}
