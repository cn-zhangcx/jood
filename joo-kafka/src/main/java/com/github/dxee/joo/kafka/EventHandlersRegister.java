package com.github.dxee.joo.kafka;

import com.github.dxee.joo.eventhandling.*;
import com.github.dxee.joo.kafka.internal.EventHandlerWrapper;
import com.google.common.reflect.TypeToken;
import com.google.inject.spi.ProvisionListener;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Listeners registry
 *
 * @author bing.fan
 * 2018-07-02 14:20
 */
public class EventHandlersRegister implements EventHandlerRegister, ProvisionListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHandlersRegister.class);
    // synchronized because put may be executed in different thread than read access
    // if synchronization is found too heavy for this, extract interface and implement
    // an immutable dictionary and another modifiable one
    private final Map<String, Set<EventHandler>> eventHandlers
            = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Parser<? extends Message>> eventMessageParsers
            = Collections.synchronizedMap(new HashMap<>());
    private final ErrorHandler errorHandler;

    public EventHandlersRegister(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    @Override
    public Set<EventHandler> getEventHandler(String typeName) {
        return eventHandlers.get(typeName);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public Parser<? extends Message> getParser(String typeName) {
        return eventMessageParsers.get(typeName);
    }

    @Override
    public <T> void onProvision(ProvisionInvocation<T> provision) {
        T provisioned = provision.provision();
        eventHandlerRegistry(provisioned);
    }

    @SuppressWarnings("unchecked")
    private void eventHandlerRegistry(Object object) {
        Set<? extends Class<?>> supertypes = TypeToken.of(object.getClass()).getTypes().rawTypes();
        for (Class<?> supertype : supertypes) {
            for (Method method : supertype.getDeclaredMethods()) {
                if (method.isAnnotationPresent(EventSubscribe.class) && !method.isSynthetic()) {
                    Class<?>[] parameterTypes = method.getParameterTypes();
                    checkArgument(
                            parameterTypes.length == 2,
                            "Method %s has @EventSubscribe annotation but has %s parameters."
                                    + "Subscriber methods must have exactly 2 parameter.",
                            method,
                            parameterTypes.length);

                    Class<? extends Message> messageClass = method.getAnnotation(EventSubscribe.class).value();
                    String messageName = TypeNames.of(messageClass);

                    Set<EventHandler> eventHandlerWrappers = eventHandlers.get(messageName);
                    if (null == eventHandlerWrappers) {
                        eventHandlerWrappers = new HashSet<>();
                        eventHandlers.put(messageName, eventHandlerWrappers);
                    }
                    EventHandlerWrapper eventHandlerWrapper = new EventHandlerWrapper(object, method);
                    eventHandlerWrappers.add(eventHandlerWrapper);

                    Parser<? extends Message> messageParser = eventMessageParsers.get(messageName);
                    if (null == messageParser) {
                        eventMessageParsers.put(messageName, parser(messageClass));
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Parser<? extends Message> parser(Class<?> clazz) {
        try {
            // static method, no arguments
            Method parserMethod = clazz.getMethod("parser");
            // static method, no arguments
            return (Parser<Message>) parserMethod.invoke(null, (Object[]) null);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ignored) {
            // too noisy:
            LOGGER.debug("Ignoring protobuf type {} as we cannot invoke static method parse().",
                    TypeNames.of(clazz), ignored);
        }
        return null;
    }

}
