package com.github.dxee.woow.eventhandling;


/**
 * A Strategy interface to allow exchangeable failure handling behaviour.
 */
public interface ErrorHandler {

    /**
     * This method decides if a event event should be re-tried or not.
     * <p>
     * It may block the current thread (which is calling the message handler) if a delay between retries is required.
     *
     * @param event       the event
     * @param failureCause the root cause of the failure
     * @return true if message delivery should be re-tried, false otherwise
     */
    boolean handleError(EventMessage event, Throwable failureCause);

}
