package com.github.dxee.jood.consumer;

/**
 * Throw when consume kafka message error occurs
 *
 * @author bing.fan
 * 2018-08-02 14:25
 */
public class ConsumerException extends RuntimeException {
    public ConsumerException(String message) {
        super(message);
    }

    public ConsumerException(String message, Throwable ex) {
        super(message, ex);
    }
}
