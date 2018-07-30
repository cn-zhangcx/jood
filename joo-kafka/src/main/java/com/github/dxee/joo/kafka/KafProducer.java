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
 * KafProducer is a kafka EventBus for dispatching EventMessage
 *
 * @author bing.fan
 * 2018-07-06 18:17
 */
public class KafProducer implements EventBus {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafProducer.class);

    private final KafkaProducer<String, byte[]> kafkaProducer;

    public KafProducer(Properties kafkaProducerConfig) {
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

        LOGGER.info("Shut down producer.");
    }

    public void publish(EventMessage eventMessage) {
        String destinationTopic = eventMessage.getMetaData().getTopic();
        String partitioningKey = eventMessage.getMetaData().getPartitioningKey();
        Envelope envelope = EventMessages.toKafka(eventMessage);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(destinationTopic, partitioningKey,
                envelope.toByteArray());

        try {
            kafkaProducer.send(record).get();
        } catch (InterruptedException ex) {
            LOGGER.warn("KafProducer interrupted while waiting on future.get() of kafkaProducer.publish(record). "
                    + "It is unknown if the eventMessage has been sent.", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            LOGGER.warn("Error sending eventMessage", cause);
            // Examples for Exceptions seen during testing:
            // org.apache.kafkaProducer.common.errors.NetworkException:
            // The server disconnected before a response was received.
            // org.apache.kafkaProducer.common.errors.TimeoutException:
            // Expiring 1 record(s) for ping-2 due to 30180 ms has passed since batch creation plus linger time
            // org.apache.kafkaProducer.common.errors.UnknownTopicOrPartitionException:
            // This server does not host this topic-partition.
            // org.apache.kafkaProducer.common.errors.NotLeaderForPartitionException:
            // This server is not the leader for that topic-partition.

            // The error handling strategy here is to not retry here but pass to the caller:
            // If for example the producer is used in a synchronous context, it probably does not make sense to retry.
            // However, in an asynchronous context (e.g. in a EventHandler) it would be wise to retry.
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(ex);
            }
        }
    }
}

