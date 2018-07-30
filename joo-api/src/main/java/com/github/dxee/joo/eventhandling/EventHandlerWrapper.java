package com.github.dxee.joo.eventhandling;

import com.github.dxee.joo.JooContext;
import com.google.common.base.Objects;
import com.google.protobuf.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Event handler wrapper
 *
 * @author bing.fan
 * 2018-07-10 10:24
 */
public final class EventHandlerWrapper<T extends Message> implements EventHandler<T> {

    private final Object object;
    private final Method method;

    public EventHandlerWrapper(Object object, Method method) {
        this.object = object;
        this.method = method;
    }

    @Override
    public void handle(EventMessage<T> eventMessage, JooContext context)
            throws InvocationTargetException, IllegalAccessException {
        method.invoke(object, eventMessage, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EventHandlerWrapper that = (EventHandlerWrapper) o;

        return Objects.equal(object, that.object)
                && Objects.equal(method, that.method);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(object, method);
    }
}
