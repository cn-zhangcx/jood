package com.github.dxee.joo.kafka;

import com.github.dxee.joo.eventhandling.EventBus;
import com.github.dxee.joo.eventhandling.EventMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * KafkaPublisher is a kafka EventBus for dispatching EventMessage
 *
 * @author bing.fan
 * 2018-07-06 18:17
 */
public class KafkaPublisher implements EventBus {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublisher.class);

    private final KafkaProducer<String, byte[]> kafkaProducer;

    public KafkaPublisher(Properties kafkaProducerConfig) {
        // Mandatory settings, not changeable.
        kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
        LOGGER.info("Created producer.");
    }

    @PreDestroy
    public void shutdown() {
        try {
            kafkaProducer.close(90, TimeUnit.SECONDS);
        } catch (Exception unexpected) {
            LOGGER.warn("Ignored unexpected exception in producer shut down", unexpected);
        }

        LOGGER.info("Shutdown publisher successfully.");
    }

    @Override
    public void publish(EventMessage eventMessage) {
        String destinationTopic = eventMessage.getMetaData().getTopic();
        String partitioningKey = eventMessage.getMetaData().getPartitioningKey();
        Envelope envelope = EventMessages.toKafka(eventMessage);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(destinationTopic, partitioningKey,
                envelope.toByteArray());

        try {
            kafkaProducer.send(record).get();
        } catch (InterruptedException ex) {
            LOGGER.warn("KafkaPublisher interrupted while waiting on future.get() of kafkaProducer.publish(record). "
                    + "It is unknown if the eventMessage has been sent.", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            LOGGER.warn("Error sending eventMessage", cause);
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(ex);
            }
        }
    }
}

