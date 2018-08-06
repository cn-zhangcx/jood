package com.github.dxee.joo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Do the partitioned record consume here, will stop when exception throws by action
 *
 * @author bing.fan
 * 2018-08-02 14:30
 */
class PartitionProcessor<K, V> implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionProcessor.class);

    private volatile boolean stopped = false;

    private final BlockingQueue<ConsumerRecord<K, V>> queue;
    private final ConsumerRecordRelay<K, V> relay;
    private final MessageConsumer<ConsumerRecord<K, V>> action;
    private final TopicPartition topicPartition;

    PartitionProcessor(TopicPartition topicPartition, ConsumerRecordRelay<K, V> relay,
                       MessageConsumer<ConsumerRecord<K, V>> action, int queueSize) {
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.relay = relay;
        this.action = action;
        this.topicPartition = topicPartition;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(topicPartition.toString());
        LOGGER.info("PartitionProcessor for {} started", topicPartition);

        ConsumerRecord<K, V> record = null;
        try {
            while (!stopped) {
                record = queue.take();
                action.accept(record);
                relay.setOffset(record);
            }
        } catch (InterruptedException ignored) {
            LOGGER.debug("PartitionProcessor for {} interrupted while waiting for messages", topicPartition);
        } catch (Exception ex) {
            LOGGER.error("Exception during processing topic partition {}, consumer record {}. Stopping!",
                    topicPartition, record, ex);
        }
        stop();
        queue.clear();
        LOGGER.info("PartitionProcessor for {} stopped", topicPartition);
    }

    public void stop() {
        stopped = true;
    }

    public void queue(ConsumerRecord<K, V> record) throws InterruptedException {
        queue.put(record);
    }

    public boolean isStopped() {
        return stopped;
    }
}
