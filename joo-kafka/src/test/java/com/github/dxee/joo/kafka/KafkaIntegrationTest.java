package com.github.dxee.joo.kafka;

import com.github.dxee.dject.Dject;
import com.github.dxee.dject.ext.ShutdownHookModule;
import com.github.dxee.joo.JooContext;
import com.github.dxee.joo.kafka.embedded.KafkaCluster;
import com.github.dxee.joo.eventhandling.*;
import com.github.dxee.joo.test.IntegrationTest;
import com.github.dxee.joo.test.TestUtils;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Stage;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * KafkaIntegrationTest
 *
 * @author bing.fan
 * 2018-07-11 23:46
 */
@Category(IntegrationTest.class)
public class KafkaIntegrationTest {
    static int zkPort = -1;
    static int kafkaBrokerPort = -1;
    static KafkaCluster cluster = null;

    @BeforeClass
    public static void setUp() {
        zkPort = TestUtils.freePort();
        kafkaBrokerPort = TestUtils.freePort(zkPort);

        cluster = KafkaCluster.newBuilder()
                .withZookeeper("127.0.0.1", zkPort)
                .withBroker(1, "127.0.0.1", kafkaBrokerPort)
                .build();
        cluster.start();
    }

    @AfterClass
    public static void tearDown() throws InterruptedException {
        cluster.shutdown();
    }

    @Rule
    public TestName name = new TestName();

    @Before
    public void printTestHeader() {
        System.out.println("\n=======================================================");
        System.out.println("  Running Test : " + name.getMethodName());
        System.out.println("=======================================================\n");
    }

    @Singleton
    public static class EventHandler1 {
        private final EventBus eventBus;
        private final CountDownLatch requestLatch;

        @Inject
        public EventHandler1(EventBus eventBus, @Named("requestLatch")CountDownLatch requestLatch) {
            this.eventBus = eventBus;
            this.requestLatch = requestLatch;
        }

        @EventSubscribe(SayHelloToCmd.class)
        public void test(EventMessage<SayHelloToCmd> sayHelloToCmdEventMessage, JooContext jooContext) {
            SayHelloToReply greeting = SayHelloToReply.newBuilder()
                    .setGreeting("Hello to " + sayHelloToCmdEventMessage.getPayload().getName())
                    .build();
            EventMessage reply = EventMessages.replyTo(sayHelloToCmdEventMessage, greeting, jooContext);

            eventBus.publish(reply);
            requestLatch.countDown();
        }
    }

    @Singleton
    public static class EventHandler2 {
        private final CountDownLatch responseLatch;

        @Inject
        public EventHandler2(@Named("responseLatch")CountDownLatch responseLatch) {
            this.responseLatch = responseLatch;
        }

        @EventSubscribe(SayHelloToReply.class)
        public void test(EventMessage<SayHelloToReply> sayHelloToReplyEventMessage, JooContext jooContext) {
            responseLatch.countDown();
        }
    }

    @Test
    public void simple_producer_consumer() throws InterruptedException {
        cluster.createTopic("ping", 1);
        cluster.createTopic("pong", 1);

        String ping = "ping";
        String pong = "pong";

        final int N = 10;
        final CountDownLatch requestLatch = new CountDownLatch(N);
        final CountDownLatch responseLatch = new CountDownLatch(N);
        Dject dject = Dject.newBuilder().withModules(new AbstractModule() {
            @Override
            protected void configure() {
                EventHandlersRegister eventHandlersRegister =
                        new EventHandlersRegister(new DiscardFailedMessages());
                bind(EventHandlerRegister.class).toInstance(eventHandlersRegister);
                bindListener(Matchers.any(), eventHandlersRegister);

                bind(EventHandler1.class).asEagerSingleton();
                bind(EventHandler2.class).asEagerSingleton();

                bind(CountDownLatch.class).annotatedWith(Names.named("requestLatch")).toInstance(requestLatch);
                bind(CountDownLatch.class).annotatedWith(Names.named("responseLatch")).toInstance(responseLatch);
            }

            @Provides
            @Singleton
            @Named("requestEventProcessor")
            public EventProcessor requestEventProcessor(EventHandlerRegister eventHandlerRegister) {
                return consumerForTopic(ping, kafkaBrokerPort,
                        eventHandlerRegister);
            }

            @Provides
            @Singleton
            @Named("replayEventProcessor")
            public EventProcessor replayEventProcessor(EventHandlerRegister eventHandlerRegister) {
                return consumerForTopic(pong, kafkaBrokerPort,
                        eventHandlerRegister);
            }

            @Provides
            @Singleton
            public EventBus eventBus() {
                Properties properties = new Properties();
                properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + kafkaBrokerPort);
                KafkaPublisher kafkaPublisher = new KafkaPublisher(properties);
                return kafkaPublisher;
            }
        }, new ShutdownHookModule()).withStage(Stage.PRODUCTION).build();

        // Key.get(EventProcessor.class, Names.named("replayEventProcessor"));
        EventBus eventBus = dject.getInstance(EventBus.class);
        for (int i = 0; i < N; i++) {
            SayHelloToCmd cmd = SayHelloToCmd.newBuilder().setName(Integer.toString(i)).build();
            EventMessage request = EventMessages.requestFor(ping, pong, null, cmd, new JooContext());
            eventBus.publish(request);
        }

        assertTrue(requestLatch.await(60, TimeUnit.SECONDS));
        assertTrue(responseLatch.await(60, TimeUnit.SECONDS));
    }

    public EventProcessor consumerForTopic(String topic, int port,
                                           EventHandlerRegister eventHandlerRegister) {
        String consumerGroupId = defaultConsumerGroupId(topic);

        EventProcessor eventProcessor = new KafkaProcessor(topic, consumerGroupId, defaultKafkaConfig(port),
                eventHandlerRegister);

        return eventProcessor;
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