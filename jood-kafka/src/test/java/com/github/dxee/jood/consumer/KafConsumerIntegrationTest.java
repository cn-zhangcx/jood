package com.github.dxee.jood.consumer;

import com.github.dxee.jood.embedded.KafkaCluster;
import com.github.dxee.jood.IntegrationTest;
import com.github.dxee.jood.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.awaitility.Awaitility.await;

@Category(IntegrationTest.class)
public class KafConsumerIntegrationTest {
    private static final int NUMBER_OF_MESSAGES = 50;
    private static final String TEST_GROUP = "TestGroup";

    private Properties props;

    int zkPort = -1;
    int kafkaBrokerPort = -1;
    KafkaCluster cluster = null;

    @Before
    public void setUp() {
        zkPort = TestUtils.freePort();
        kafkaBrokerPort = TestUtils.freePort(zkPort);

        cluster = KafkaCluster.newBuilder()
                .withZookeeper("127.0.0.1", zkPort)
                .withBroker(1, "127.0.0.1", kafkaBrokerPort)
                .build();
        cluster.start();

        props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + kafkaBrokerPort);
        props.setProperty(GROUP_ID_CONFIG, TEST_GROUP);
        props.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }

    @After
    public void tearDown() throws InterruptedException {
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

    @Test
    public void send_and_receive() throws Exception {
        final String topic = "my_topic";
        cluster.createTopic(topic, 20, 1);

        AtomicInteger messageCounter = new AtomicInteger();
        MessageConsumer<ConsumerRecord<String, String>> action = (message) -> messageCounter.incrementAndGet();

        KafConsumer<String, String> consumer = new KafConsumer<>(topic, props, 42, action);
        consumer.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", topic,
                "127.0.0.1:" + kafkaBrokerPort);

        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            testProducer.send(String.valueOf(i));
        }

        await().atMost(5, SECONDS).until(() -> messageCounter.get() == NUMBER_OF_MESSAGES);
        testProducer.close();
        consumer.stop();
    }

    @Test
    public void send_and_receive_with_exception() throws Exception {
        final String topic = "bad_consumer_topic";
        cluster.createTopic(topic, 20);

        AtomicInteger exceptionCounter = new AtomicInteger();
        AtomicInteger messageCounter = new AtomicInteger();
        MessageConsumer<ConsumerRecord<String, String>> action0 = (message) -> {
            if (exceptionCounter.incrementAndGet() > NUMBER_OF_MESSAGES/3) {
                throw new RuntimeException("Bad consumer");
            }
            messageCounter.incrementAndGet();
        };

        MessageConsumer<ConsumerRecord<String, String>> action1 = (message) -> messageCounter.incrementAndGet();

        KafConsumer<String, String> consumer = new KafConsumer<>(topic, props, 42, action0);
        consumer.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", topic,
                "127.0.0.1:" + kafkaBrokerPort);

        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            testProducer.send(String.valueOf(i));
        }

        KafConsumer<String, String> anotherConsumer = new KafConsumer<>(topic, props, 42, action1);
        anotherConsumer.start();

        await().atMost(10, SECONDS).until(() -> messageCounter.get() >= NUMBER_OF_MESSAGES);

        testProducer.close();
        anotherConsumer.stop();
    }

    @Test
    public void send_and_receive_with_multipartitions() throws Exception {
        final String topic = "multip_consumer_topic";
        cluster.createTopic(topic, 20, 1);

        AtomicInteger messageCounter = new AtomicInteger();

        final MessageConsumer<ConsumerRecord<String, String>> action = (message) -> messageCounter.incrementAndGet();

        KafConsumer<String, String> consumer1 = new KafConsumer<>(topic, props, 42, action);
        consumer1.start();

        KafConsumer<String, String> consumer2 = new KafConsumer<>(topic, props, 42, action);
        consumer2.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", topic,
                "127.0.0.1:" + kafkaBrokerPort);

        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            testProducer.send(String.valueOf(i));
        }

        await().atMost(10, SECONDS).until(() -> messageCounter.get() >= NUMBER_OF_MESSAGES);
        testProducer.close();
        consumer1.stop();
        consumer2.stop();
    }

    @Test
    public void reassignment() throws Exception {
        final String topic = "reassignment_topic";
        cluster.createTopic(topic, 20, 1);

        AtomicInteger messageCounter = new AtomicInteger();
        MessageConsumer<ConsumerRecord<String, String>> action = (message) -> messageCounter.incrementAndGet();

        KafConsumer<String, String> consumer = new KafConsumer<>(topic, props, 42, action);
        consumer.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", topic,
                "127.0.0.1:" + kafkaBrokerPort);
        KafConsumer<String, String> anotherConsumer = null;
        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            testProducer.send(String.valueOf(i));
            if (i == NUMBER_OF_MESSAGES / 10) {
                anotherConsumer = new KafConsumer<>(topic, props, 42, action);
                anotherConsumer.start();
            }
            if (i == NUMBER_OF_MESSAGES / 5) {
                consumer.stop();
            }
        }

        await().atMost(5, SECONDS).until(() -> messageCounter.get() >= NUMBER_OF_MESSAGES);
        testProducer.close();
        anotherConsumer.stop();
    }

    @Test
    public void sendOneMessage_restartConsumer_ensureOneMessageOnly() throws Exception {
        final String topic = "low_load_topic";
        final String group = "OneMessageGroup";
        props.setProperty(GROUP_ID_CONFIG, group);

        cluster.createTopic(topic, 20, 1);

        AtomicInteger messageCounter = new AtomicInteger();
        MessageConsumer<ConsumerRecord<String, String>> action = (message) -> messageCounter.incrementAndGet();

        KafConsumer<String, String> consumer0 = new KafConsumer<>(topic, props, 5, action);
        consumer0.start();

        SimpleTestProducer testProducer = new SimpleTestProducer("Lorem-Radio", topic,
                "127.0.0.1:" + kafkaBrokerPort);
        testProducer.send("test");

        await().atMost(5, SECONDS).until(() -> messageCounter.get() == 1);
        consumer0.stop();

        KafConsumer<String, String> consumer1 = new KafConsumer<String, String>(topic, props, 5, action);
        consumer1.start();

        await().atMost(5, SECONDS).until(() -> messageCounter.get() == 1);
        consumer1.stop();
    }
}
