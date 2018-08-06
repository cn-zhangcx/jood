package com.github.dxee.jood.consumer;

import com.google.common.base.Joiner;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Relay ConsumerRecords here.
 *
 * @author bing.fan
 * 2018-08-02 14:26
 */
class ConsumerRecordRelay<K, V> implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRecordRelay.class);
    private static final Duration POLL_BLOCK_MILLIS = Duration.ofMillis(300L);

    private volatile boolean stopped = false;
    private volatile boolean updateOffsets = false;

    private final Map<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
    private final KafConsumer<K, V> kafConsumer;
    private final Consumer<K, V> consumer;

    public ConsumerRecordRelay(Consumer<K, V> consumer, KafConsumer<K, V> kafConsumer) {
        this.kafConsumer = kafConsumer;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        while (!stopped) {
            consumer.poll(POLL_BLOCK_MILLIS).forEach(this::relayRecordToConsumerHandlingErrors);
            commitOffsets();
        }
        consumer.close();
        LOGGER.info("Kafka message relay stopped");
    }

    private void relayRecordToConsumerHandlingErrors(ConsumerRecord<K, V> record) {
        try {
            kafConsumer.relay(record);
        } catch (InterruptedException ignored) {
            LOGGER.info("Interrupted during relay");
        } catch (Exception ex) {
            LOGGER.error("Error while relaying messages from kafka to queue, topic {}, partition {}",
                    record.topic(),
                    record.partition(),
                    ex);
            kafConsumer.stop();
        }
    }

    public void setOffset(ConsumerRecord<K, V> record) {
        offsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );
        updateOffsets = true;
    }

    public void removePartitionFromOffset(TopicPartition topicPartition) {
        offsets.remove(topicPartition);
    }

    private void commitOffsets() {
        if (updateOffsets) {
            consumer.commitAsync(offsets, this::callback);
            updateOffsets = false;
        }
    }

    void stop() {
        LOGGER.info("Stopping Kafka message relay");
        stopped = true;
    }

    private void callback(Map<TopicPartition, OffsetAndMetadata> offsets, Exception ex) {
        Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator("=");
        if (ex != null) {
            LOGGER.error("Error during offsets commit: '{}'", mapJoiner.join(offsets), ex);
        } else {
            LOGGER.trace("Commit offsets successfully, {}", mapJoiner.join(offsets));
        }
    }
}
