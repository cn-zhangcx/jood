package com.github.dxee.woow.kafka.producer;

import com.github.dxee.woow.WoowContext;
import com.github.dxee.woow.kafka.Messages;
import com.github.dxee.woow.kafka.SayHelloToCmd;
import com.github.dxee.woow.kafka.SayHelloToReply;
import com.github.dxee.woow.kafka.consumer.DiscardFailedMessages;
import com.github.dxee.woow.kafka.consumer.FailedMessageProcessor;
import com.github.dxee.woow.kafka.consumer.KafConsumer;
import com.github.dxee.woow.kafka.consumer.PartitionProcessorFactory;
import com.github.dxee.woow.kafka.embedded.KafkaCluster;
import com.github.dxee.woow.messaging.*;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class KafProducerTest {
    @Test
    public void simpleProducerConsumer() throws InterruptedException {
        KafkaCluster cluster = KafkaCluster.newBuilder()
                .withZookeeper("127.0.0.1", 2181)
                .withBroker(1, "127.0.0.1", 9092)
                .build();

        cluster.start();
        cluster.createTopic("ping", 1);
        cluster.createTopic("pong", 1);

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        KafProducer kafProducer = new KafProducer(properties);

        Topic ping = new Topic("ping");
        Topic pong = new Topic("pong");

        final int N = 10;

        for (int i = 0; i < N; i++) {
            SayHelloToCmd cmd = SayHelloToCmd.newBuilder().setName(Integer.toString(i)).build();
            EventMessage request = Messages.requestFor(ping, pong, "1", cmd, new WoowContext());
            kafProducer.send(request);
        }

        final CountDownLatch requestLatch = new CountDownLatch(N);
        final CountDownLatch responseLatch = new CountDownLatch(N);

        TypeDictionary typeDictionary = new TypeDictionary();

        typeDictionary.putHandler(MessageType.of(SayHelloToCmd.class),
                (MessageHandler<SayHelloToCmd>) (message, context) -> {
                    SayHelloToReply greeting = SayHelloToReply.newBuilder()
                            .setGreeting("Hello to " + message.getPayload().getName())
                            .build();
                    EventMessage reply = Messages.replyTo(message, greeting, context);

                    kafProducer.send(reply);
                    requestLatch.countDown();

                });
        typeDictionary.putParser(MessageType.of(SayHelloToCmd.class), parser(SayHelloToCmd.class));

        typeDictionary.putHandler(
                MessageType.of(SayHelloToReply.class),
                (MessageHandler<SayHelloToReply>) (message, context) -> responseLatch.countDown()
        );
        typeDictionary.putParser(MessageType.of(SayHelloToReply.class), parser(SayHelloToReply.class));


        PartitionProcessorFactory partitionProcessorFactory = new PartitionProcessorFactory(
                typeDictionary,
                new DiscardFailedMessages()
        );

        final KafConsumer requestConsumer = consumerForTopic(ping, typeDictionary, new DiscardFailedMessages());
        final KafConsumer replyConsumer = consumerForTopic(pong, typeDictionary, new DiscardFailedMessages());

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

    public KafConsumer consumerForTopic(Topic topic, TypeDictionary typeDictionary,
                                        DiscardFailedMessages failedMessageStrategy) {
        String consumerGroupId = defaultConsumerGroupId(topic);

        return new KafConsumer(topic, consumerGroupId, defaultKafkaConfig(),
                defaultPartitionProcessorFactory(typeDictionary, failedMessageStrategy));
    }

    private String defaultConsumerGroupId(Topic topic) {
        // default consumer group id consists of topic and service name
        return topic.topic() + "-" + "com.sixt.service.unknown";
    }

    private PartitionProcessorFactory defaultPartitionProcessorFactory(TypeDictionary typeDictionary,
                                                                       FailedMessageProcessor failedMessageStrategy) {
        PartitionProcessorFactory partitionProcessorFactory = new PartitionProcessorFactory(typeDictionary,
                failedMessageStrategy);
        return partitionProcessorFactory;
    }

    private Properties defaultKafkaConfig() {
        String kafkaBootstrapServers = "127.0.0.1:9092";

        Properties kafkaConfig = new Properties();
        kafkaConfig.put("bootstrap.servers", kafkaBootstrapServers);

        // The heartbeat is send in the background by the client library itself
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