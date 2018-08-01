package com.github.dxee.joo.kafka;

import com.github.dxee.joo.JooContext;
import com.github.dxee.joo.eventhandling.*;
import com.github.dxee.joo.kafka.consumer.ConsumerException;
import com.github.dxee.joo.kafka.consumer.KafConsumer;
import com.github.dxee.joo.kafka.consumer.KafRecordConsumer;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka event processor for receive and excute event handler.
 *
 * @author bing.fan
 * 2018-08-02 14:38
 */
public class KafkaProcessor implements EventProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProcessor.class);

    private final KafConsumer<String, byte[]> kafConsumer;
    private final EventHandlerRegister eventHandlerRegister;

    public KafkaProcessor(String topic, String consumerGroupId, Properties consumerProperties,
                          EventHandlerRegister eventHandlerRegister) {
        // Mandatory settings, not changeable
        consumerProperties.put("group.id", consumerGroupId);
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());

        kafConsumer = new KafConsumer<>(topic, consumerProperties, 1024,
                new RecordConsumer());
        this.eventHandlerRegister = eventHandlerRegister;
    }

    @PostConstruct
    @Override
    public void start() {
        kafConsumer.start();
    }

    @PreDestroy
    @Override
    public void shutdown() {
        kafConsumer.stop();
    }

    @Override
    public EventHandlerRegister getEventHandlerRegister() {
        return eventHandlerRegister;
    }

    class RecordConsumer implements KafRecordConsumer<ConsumerRecord<String, byte[]>> {

        @Override
        public void accept(ConsumerRecord<String, byte[]> record) {
            try {
                EventMessage<? extends Message> eventMessage = parseEventMessage(record);
                if (eventMessage == null) {
                    // Can not even parse the eventMessage, so we give up.
                    return;
                }

                deliverToEventHandlers(eventMessage);

            } catch (Throwable unexpectedError) {
                // Anything that reaches here could be potentially a condition that the thread could not recover from.
                // see https://docs.oracle.com/javase/specs/jls/se8/html/jls-11.html#jls-11.1
                //
                // Thus, we try to log the error, but let the thread die.
                // The thread pool will create a new thread is the hosting process itself is still alive.
                LOGGER.error("Unexpected error while handling message", unexpectedError);
                throw unexpectedError;
            }
        }

        private EventMessage<? extends Message> parseEventMessage(ConsumerRecord<String, byte[]> record) {
            Envelope envelope = null;

            try {
                envelope = Envelope.parseFrom(record.value());
            } catch (InvalidProtocolBufferException parseError) {
                parsingFailed(envelope, parseError);
                return null;
            }

            try {
                String type = envelope.getTypeName();

                Parser<? extends Message> parser = eventHandlerRegister.getParser(type);
                if (parser == null) {
                    throw new ConsumerException("Could not parse type:" + type);
                }

                Message innerMessage = parser.parseFrom(envelope.getInnerMessage());
                return EventMessages.fromKafka(innerMessage, envelope, record);
            } catch (InvalidProtocolBufferException | ConsumerException unrecoverableParsingError) {
                parsingFailed(envelope, unrecoverableParsingError);
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        private void deliverToEventHandlers(EventMessage<? extends Message> eventMessage) {
            String typeName = eventMessage.getMetaData().getTypeName();
            Set<EventHandler> eventHandlers
                    = eventHandlerRegister.getEventHandler(typeName);
            ErrorHandler errorHandler
                    = eventHandlerRegister.getErrorHandler();

            eventHandlers.forEach(eventHandler ->
                    deliverToEventHandler(eventMessage, eventHandler, errorHandler)
            );
        }

        private void deliverToEventHandler(EventMessage<? extends Message> eventMessage,
                                           EventHandler eventHandler,
                                           ErrorHandler errorHandler) {
            JooContext context = eventMessage.getMetaData().newContextFromMetadata();

            boolean deliveryFailed = false;
            try {
                if (eventHandler == null) {
                    throw new IllegalArgumentException(eventMessage.getMetaData().getTypeName());
                }
                deliveryStarted(eventMessage, eventHandler, context);
                // Leave the framework here: hand over execution to service-specific eventHandler.
                eventHandler.handle(eventMessage, context);
            } catch (Exception failure) {
                deliveryFailed = true;
                // Strategy decides: Should we retry to deliver the failed eventMessage?
                errorHandler.handleError(eventMessage, eventHandler, failure);
                deliveryFailed(eventMessage, eventHandler, failure);
            } finally {
                deliveryEnded(eventMessage, eventHandler, deliveryFailed);
            }
        }


        // Helper methods to get the glue code for debug logging, tracing and metrics out of the main control flow
        private void parsingFailed(Envelope envelope, Exception parseException) {
            // TODO log, metrics
            LOGGER.warn("parsingFailed {}", envelope, parseException);
        }


        private void deliveryStarted(EventMessage message, EventHandler handler, JooContext context) {
            // TODO log, trace, metrics
            LOGGER.debug("deliveryStarted {}, {}, {}", message, handler.getClass().getName(), context);
        }

        private void deliveryFailed(EventMessage message, EventHandler failedEventHandler,
                                    Exception failure) {
            // TODO log, metrics
            LOGGER.warn("deliveryFailed {}, event handler {}", message, failedEventHandler, failure);
        }

        private void deliveryEnded(EventMessage message, EventHandler failedEventHandler,
                                   boolean deliveryFailed) {
            // TODO log, trace, metrics
            if (!deliveryFailed) {
                LOGGER.debug("deliveryEnded {}", message);
                return;
            }
            LOGGER.error("deliveryEnded {}, failedEventHandler {}", message, failedEventHandler);
        }
    }
}
