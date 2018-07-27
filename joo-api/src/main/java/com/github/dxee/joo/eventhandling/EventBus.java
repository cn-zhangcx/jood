package com.github.dxee.joo.eventhandling;

/**
 *  Specification of the mechanism on which the Event Listeners can subscribe for events and
 *  event publishers can publish their events. The event bus dispatches events to all subscribed listeners.
 *
 * @author bing.fan
 * 2018-07-06 18:34
 */
public interface EventBus {
    /**
     * Method Comment Here
     *
     * @param eventMessage The EventMessage to publish
     */
    void publish(EventMessage eventMessage);
}
