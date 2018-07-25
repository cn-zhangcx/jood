package com.github.dxee.woow.kafka;


import com.github.dxee.woow.eventhandling.MessageType;

public class UnknownMessageHandlerException extends Exception {
    public UnknownMessageHandlerException(MessageType type) {
        super(type.toString());
    }
}
