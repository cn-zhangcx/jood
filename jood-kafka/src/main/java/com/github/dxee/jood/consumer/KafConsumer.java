package com.github.dxee.jood.consumer;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

/**
 * This class leverages the kafka-clients consumer implementation to distribute messages from assigned partitions to
 * {@link java.util.concurrent.BlockingQueue}s. Each assigned partition will relay its messages to its own queue.
 * In addition, each queue has a consuming process/thread. Once a message has been "processed" successfully its offset
 * will be marked to be committed.
 *
 * @param <K> Key type
 * @param <V> Value type
 * @author bing.fan
 * 2018-08-02 14:18
 */
public class KafConsumer<K, V> implements ConsumerRebalanceListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafConsumer.class);

    private final Properties kafkaConfig;
    private final String topic;
    private final int queueSize;
    private final MessageConsumer<ConsumerRecord<K, V>> action;

    private final Map<Integer, PartitionProcessor<K, V>> processors = new ConcurrentHashMap<>();
    private final ExecutorService pool;
    private final Consumer<K, V> consumer;

    private final Object lock = new Object();
    private volatile ConsumerRecordRelay<K, V> relay;

    public KafConsumer(String topic, Properties kafkaConfig, int queueSize,
                       MessageConsumer<ConsumerRecord<K, V>> action) {
        this.topic = topic;
        this.kafkaConfig = KafConsumerConfigValidator.validate(kafkaConfig);
        this.action = action;
        this.queueSize = queueSize;
        this.pool = Executors.newCachedThreadPool();
        this.consumer = createConsumer();
    }

    public void start() {
        synchronized (lock) {
            if (relay != null) {
                return;
            }

            // Connects to the kafka
            consumer.subscribe(Collections.singletonList(topic), this);

            // Start relay
            relay = new ConsumerRecordRelay<>(consumer, this);
            new Thread(relay, topic).start();
        }
    }

    public void stop() {
        synchronized (lock) {
            // Stop relay first
            if (relay != null) {
                relay.stop();
            }

            // Stop processor pool second
            if (!MoreExecutors.shutdownAndAwaitTermination(pool, 60, SECONDS)) {
                LOGGER.error("Pool was not terminated properly.");
            }
        }
    }

    /**
     * Should only be called by {@link ConsumerRecordRelay}
     */
    void relay(ConsumerRecord<K, V> record) throws InterruptedException {
        if (!topic.equals(record.topic())) {
            throw new ConsumerException(String.format("Message from unexpected topic %s", record.topic()));
        }

        PartitionProcessor<K, V> partitionProcessor = processors.get(record.partition());
        if (partitionProcessor.isStopped()) {
            throw new ConsumerException(String.format("PartitionProcessor is stopped, could not consume record %s",
                    record));
        }

        partitionProcessor.queue(record);
    }

    private Consumer<K, V> createConsumer() {
        setDefaultPropertyIfNotPresent(kafkaConfig, CLIENT_ID_CONFIG, this::getClientId);
        setDefaultPropertyIfNotPresent(kafkaConfig, ENABLE_AUTO_COMMIT_CONFIG, () -> "false");
        setDefaultPropertyIfNotPresent(kafkaConfig, AUTO_OFFSET_RESET_CONFIG, () -> "earliest");
        return new KafkaConsumer<K, V>(kafkaConfig);
    }

    private void setDefaultPropertyIfNotPresent(Properties props, String propertyName, Supplier<String> defaultValue) {
        if (props.contains(propertyName)) {
            return;
        }
        props.put(propertyName, defaultValue.get());
    }

    private String getClientId() {
        try {
            return String.format("%s-%s", InetAddress.getLocalHost().getHostName(), topic);
        } catch (UnknownHostException ex) {
            throw new ConsumerException("Could not retrieve client identifier", ex);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        partitions.forEach(partition -> {
            PartitionProcessor<K, V> partitionProcessor =
                    new PartitionProcessor<>(partition, relay, action, queueSize);
            pool.execute(partitionProcessor);
            processors.put(partition.partition(), partitionProcessor);
        });
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        partitions.forEach(partition -> {
            PartitionProcessor<K, V> partitionProcessor = processors.get(partition.partition());
            partitionProcessor.stop();
            processors.remove(partition.partition());
            relay.removePartitionFromOffset(partition);
        });
    }
}
