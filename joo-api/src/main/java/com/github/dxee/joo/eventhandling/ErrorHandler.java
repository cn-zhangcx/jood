package com.github.dxee.joo.eventhandling;

/**
 * A Strategy interface to allow exchangeable failure handling behaviour.
 *
 * @author bing.fan
 * 2018-07-05 17:29
 */
public interface ErrorHandler {

    /**
     * This method decides if a event message should be re-tried or not.
     * <p>
     * It may block the current thread (which is calling the message handler) if a delay between retries is required.
     *
     * @param eventMessage       the event
     * @param failureCause the root cause of the failure
     * @return true if message delivery should be re-tried, false otherwise
     */
    boolean handleError(EventMessage eventMessage, Throwable failureCause);

}
