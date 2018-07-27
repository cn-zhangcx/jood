package com.github.dxee.joo.kafka;

/**
 * UnknownMessageTypeException
 *
 * @author bing.fan
 * 2018-07-09 09:20
 */
public class UnknownMessageTypeException extends Exception {
    public UnknownMessageTypeException(String type) {
        super(type.toString());
    }
}
