package com.github.dxee.joo.eventhandling;

/**
 * Event processor
 *
 * @author bing.fan
 * 2018-07-06 18:25
 */
public interface EventProcessor {
    /**
     * Start processing events.
     */
    void start();

    /**
     * Stop processing events.
     */
    void shutdown();
}
