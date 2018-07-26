package com.github.dxee.woow.kafka.producer;

import com.github.dxee.woow.WoowContext;
import com.github.dxee.woow.kafka.*;
import com.github.dxee.woow.kafka.consumer.AssignedPartitions;
import com.github.dxee.woow.kafka.consumer.DiscardFailedMessages;
import com.github.dxee.woow.kafka.consumer.EventListeners;
import com.github.dxee.woow.kafka.embedded.KafkaCluster;
import com.github.dxee.woow.eventhandling.*;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * KafProducerTest
 *
 * @author bing.fan
 * 2018-07-11 23:46
 */
public class KafProducerTest {

    @Test
    public void simpleProducerConsumer() throws InterruptedException {
        int zkPort = TestUtils.getAvailablePort();
        int kafkaBrokerPort = TestUtils.getAvailablePort(zkPort);
        KafkaCluster cluster = KafkaCluster.newBuilder()
                .withZookeeper("127.0.0.1", zkPort)
                .withBroker(1, "127.0.0.1", kafkaBrokerPort)
                .build();

        cluster.start();
        cluster.createTopic("ping", 1);
        cluster.createTopic("pong", 1);

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + kafkaBrokerPort);
        KafProducer kafProducer = new KafProducer(properties);

        String ping = "ping";
        String pong = "pong";

        final int N = 10;

        for (int i = 0; i < N; i++) {
            SayHelloToCmd cmd = SayHelloToCmd.newBuilder().setName(Integer.toString(i)).build();
            EventMessage request = EventMessages.requestFor(ping, pong, "1", cmd, new WoowContext());
            kafProducer.publish(request);
        }

        final CountDownLatch requestLatch = new CountDownLatch(N);
        final CountDownLatch responseLatch = new CountDownLatch(N);

        EventListeners eventListeners = new EventListeners();

        eventListeners.addListener(SayHelloToCmd.class.getTypeName(),
                (EventListener<SayHelloToCmd>) (message, context) -> {
                    SayHelloToReply greeting = SayHelloToReply.newBuilder()
                            .setGreeting("Hello to " + message.getPayload().getName())
                            .build();
                    EventMessage reply = EventMessages.replyTo(message, greeting, context);

                    kafProducer.publish(reply);
                    requestLatch.countDown();

                });
        eventListeners.addParser(SayHelloToCmd.class.getTypeName(), parser(SayHelloToCmd.class));

        eventListeners.addListener(SayHelloToReply.class.getTypeName(),
                (EventListener<SayHelloToReply>) (message, context) -> responseLatch.countDown()
        );
        eventListeners.addParser(SayHelloToReply.class.getTypeName(), parser(SayHelloToReply.class));

        final KafConsumer requestConsumer = consumerForTopic(ping, kafkaBrokerPort,
                eventListeners, new DiscardFailedMessages());
        final KafConsumer replyConsumer = consumerForTopic(pong, kafkaBrokerPort,
                eventListeners, new DiscardFailedMessages());

        requestConsumer.start();
        replyConsumer.start();

        assertTrue(requestLatch.await(60, TimeUnit.SECONDS));

        assertTrue(responseLatch.await(60, TimeUnit.SECONDS));

        kafProducer.shutdown();
        requestConsumer.shutdown();
        replyConsumer.shutdown();
    }

    @SuppressWarnings("unchecked")
    private Parser<Message> parser(Class clazz) {
        try {
            java.lang.reflect.Method method = clazz.getMethod("parser");
            return (Parser<com.google.protobuf.Message>) method.invoke(null, (Object[]) null);

        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ignored) {
            // too noisy: logger.debug("Ignoring protobuf type {}
            // as we cannot invoke static method parse().", clazz.getTypeName());
        }
        return null;
    }

    public KafConsumer consumerForTopic(String topic, int port, EventListeners eventListeners,
                                        DiscardFailedMessages failedMessageStrategy) {
        String consumerGroupId = defaultConsumerGroupId(topic);

        return new KafConsumer(topic, consumerGroupId, defaultKafkaConfig(port),
                new AssignedPartitions(eventListeners, failedMessageStrategy));
    }

    private String defaultConsumerGroupId(String topic) {
        // default consumer group id consists of topic and service name
        return topic + "-" + "test";
    }

    private Properties defaultKafkaConfig(int port) {
        String kafkaBootstrapServers = "127.0.0.1:" + port;

        Properties kafkaConfig = new Properties();
        kafkaConfig.put("bootstrap.servers", kafkaBootstrapServers);

        // The heartbeat is publish in the background by the client library itself
        kafkaConfig.put("heartbeat.interval.ms", "10000");
        kafkaConfig.put("session.timeout.ms", "30000");

        // Require explicit commit handling.
        kafkaConfig.put("enable.auto.commit", "false");

        // If this is a new group, start reading the topic from the beginning.
        kafkaConfig.put("auto.offset.reset", "earliest");

        // This is the actual timeout for the consumer loop thread calling poll() before Kafka rebalances the group.
        kafkaConfig.put("max.poll.interval.ms", 10000);

        return kafkaConfig;
    }
}