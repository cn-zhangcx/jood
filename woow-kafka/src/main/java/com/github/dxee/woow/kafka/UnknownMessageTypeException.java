package com.github.dxee.woow.kafka;

public class UnknownMessageTypeException extends Exception {
    public UnknownMessageTypeException(String type) {
        super(type.toString());
    }
}
