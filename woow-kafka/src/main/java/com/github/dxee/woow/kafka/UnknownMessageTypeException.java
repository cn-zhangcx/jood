package com.github.dxee.woow.kafka;

import com.github.dxee.woow.messaging.MessageType;

public class UnknownMessageTypeException extends Exception {
    public UnknownMessageTypeException(MessageType type) {
        super(type.toString());
    }
}
