package com.github.dxee.joo.kafka.embedded;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;


/**
 * {@code KafkaCluster} represents a Kafka cluster with one or more brokers
 * and a ZooKeeper server
 *
 *<pre>
 *{@code KafkaCluster cluster = KafkaCluster.newBuilder()
 *  .withZookeeper("127.0.0.1", 2181)
 *  .withBroker(1, "127.0.0.1", 9092)
 *  .withBroker(2, "127.0.0.1", 19092)
 *  .build();
 *
 *cluster.start();
 *
 *[...]
 *
 *cluster.shutdown();
 *}
 *</pre>
 */
public class KafkaCluster {

    private final Zk zk;

    private final Map<Integer, KafkaBroker> brokers;

    private final Path baseLogPath;

    /**
     * Creates a new {@link KafkaClusterBuilder}
     * @return a new {@link KafkaClusterBuilder}
     */
    public static KafkaClusterBuilder newBuilder() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        Path base = Paths.get(tmpDir)
                .resolve("kafka-cluster-unit")
                .resolve(String.valueOf(new Random().nextInt()));

        return new KafkaClusterBuilder(base);
    }

    /**
     * Get the broker connection string of this cluster
     * @return the broker connection string of this cluster
     */
    public String brokers() {
        return brokers.values().stream().map(KafkaBroker::getListener).collect(Collectors.joining(","));
    }

    private Properties defaultProperties() {
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers());

        return prop;
    }

    /**
     * Get a new {@link Consumer} for this cluster.
     * <p>{@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} is handled internally</p>
     *
     * @param properties additional consumer properties
     * @param <K> the key type
     * @param <V> the value type
     * @return a new {@link Consumer} for this cluster
     */
    public <K, V> Consumer<K, V> consumer(Properties properties) {
        Properties prop = new Properties();
        prop.putAll(defaultProperties());
        prop.putAll(properties);

        return new KafkaConsumer<>(prop);
    }

    /**
     * Get a new {@link Consumer} for this cluster
     *
     * <p>{@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} is handled internally</p>
     *
     * @param properties additional consumer properties
     * @param keyDeserializer the key deserializer
     * @param valueDeserializer the value deserializer
     * @param <K> the key type
     * @param <V> the value type
     * @return a new {@link Consumer} for this cluster
     */
    public <K, V> Consumer<K, V> consumer(Properties properties, Deserializer<K> keyDeserializer,
                                          Deserializer<V> valueDeserializer) {
        Properties prop = new Properties();
        prop.putAll(defaultProperties());
        prop.putAll(properties);

        return new KafkaConsumer<>(prop, keyDeserializer, valueDeserializer);
    }

    /**
     * Get a new {@link AdminClient} for this cluster
     *
     * <p>{@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} is handled internally</p>
     * @param properties additional admin client properties
     * @return a new {@link AdminClient} for this cluster
     */
    public AdminClient adminClient(Properties properties) {
        Properties prop = new Properties();
        prop.putAll(defaultProperties());
        prop.putAll(properties);

        return AdminClient.create(prop);
    }

    /**
     * Get a new {@link AdminClient} for this cluster with the default properties
     *
     * <p>{@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} is handled internally</p>
     * @return a new {@link AdminClient} for this cluster
     */
    public AdminClient adminClient() {
        return AdminClient.create(defaultProperties());
    }

    /**
     * Get a new {@link Producer} for this cluster
     *
     * <p>{@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} is handled internally</p>
     *
     * @param properties additional producer properties
     * @param <K> the key type
     * @param <V> the value type
     * @return a new {@link Producer} for this cluster
     */
    public <K, V> Producer<K, V> producer(Properties properties) {
        Properties prop = new Properties();
        prop.putAll(defaultProperties());
        prop.putAll(properties);

        return new KafkaProducer<>(prop);
    }

    /**
     * Get a new {@link Producer} for this cluster
     *
     * <p>{@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} is handled internally</p>
     *
     * @param properties additional producer properties
     * @param keySerializer the key serializer
     * @param valueSerializer the value serializer
     * @param <K> the key type
     * @param <V> the value type
     * @return a new {@link Producer} for this cluster
     */
    public <K, V> Producer<K, V> producer(Properties properties, Serializer<K> keySerializer,
                                          Serializer<V> valueSerializer) {
        Properties prop = new Properties();
        prop.putAll(defaultProperties());
        prop.putAll(properties);

        return new KafkaProducer<>(prop, keySerializer, valueSerializer);
    }

    /**
     * Stop a broker
     * @param broker the broker id
     */
    public void stop(int broker) {
        KafkaBroker brk = brokers.get(broker);
        if (brk == null) {
            throw new IllegalArgumentException(String.format("Broker with id %d does not exist", brk));
        }
        brk.stop();
    }

    /**
     * Shutdown this cluster
     */
    public void shutdown() {
        for (KafkaBroker broker: brokers.values()) {
            broker.stop();
        }
        this.zk.stop();

        try {
            Files.walkFileTree(baseLogPath, recursiveDeleteVisitor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Start this cluster
     */
    public void start() {
        try {
            this.zk.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Collection<KafkaBroker> values = brokers.values();
        for (KafkaBroker broker: values) {
            broker.start(brokers.values().size());
        }

        initialize();
    }

    /**
     * On first consumer poll, _consumer_offsets topic is created which, although fast,
     * may lead consumers with small polling interval to fail fetching any message.
     *
     * This method polls from __consumer_offsets in order to trigger topic creation.
     * It may add some overhead in the startup cycle, but once done, cluster internal
     * structures are then adding the real-life overhead instead.
     */
    private void initialize() {
        StringDeserializer deserializer = new StringDeserializer();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        try (Consumer<String, String> consumer = consumer(properties, deserializer, deserializer)) {
            consumer.subscribe(Arrays.asList("__consumer_offsets"));
            consumer.poll(0);
        }
    }

    /**
     * Creates a topic
     *
     * <p>This method blocks until topic creation is finished or failed</p>
     *
     * @param topic the topic name
     * @param replicas the replication factor
     * @param partitions the number of partitions
     * @throws RuntimeException when the requested topic could not be created
     */
    public void createTopic(String topic, int partitions, int replicas) {
        try (AdminClient client = adminClient()) {
            client.createTopics(Arrays.asList(new NewTopic(topic, partitions, (short) replicas))).all().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a topic (replication factor defaults to the number of available brokers
     * @param topic the topic name
     * @param partitions the number of partitions
     * @throws RuntimeException when the requested topic could not be created
     */
    public void createTopic(String topic, int partitions) {
        createTopic(topic, partitions, brokers.size());
    }

    /**
     * Deletes a topic
     *
     * <p>This method blocks until topic deletion is finished or failed</p>
     * @param topic the topic name
     * @throws RuntimeException when the requested topic could not be deleted
     */
    void deleteTopic(String topic) {
        try (AdminClient client = adminClient()) {
            client.deleteTopics(Arrays.asList(topic)).all().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private KafkaCluster(Zk zk, Map<Integer, KafkaBroker> brokers, Path baseLogPath) {
        this.zk = zk;
        this.brokers = brokers;
        this.baseLogPath = baseLogPath;
    }

    /**
     * Builder for {@link KafkaCluster}
     * @author Dimitris Mandalidis
     *
     */
    public static class KafkaClusterBuilder {

        private Zk zk;

        private Map<Integer, KafkaBroker> brokers = new HashMap<>();

        private final Path base;

        KafkaClusterBuilder(Path base) {
            this.base = base;
        }

        /**
         * Modifies this newBuilder, attaching Zookeeper server coordinates
         * @param host the hostname that Zookeeper will listen on
         * @param port the port that Zookeeper will bind on
         * @return this
         */
        public KafkaClusterBuilder withZookeeper(String host, int port) {
            return withZookeeper(host, port, 10);
        }

        /**
         * Modifies this newBuilder, attaching Zookeeper server coordinates
         * @param host the hostname that Zookeeper will listen on
         * @param port the port that Zookeeper will bind on
         * @param maxClientCnxns the maximum number of allowed client connections
         * @return this
         */
        public KafkaClusterBuilder withZookeeper(String host, int port, int maxClientCnxns) {
            Path dataDir = base.resolve("zk").resolve("data");
            Path snapDir = base.resolve("zk").resolve("log");
            zk = new Zk(dataDir, snapDir, host, port, maxClientCnxns);
            return this;
        }

        /**
         * Modifies this newBuilder, attaching a new Kafka broker
         * @param id the broker id
         * @param host the hostname that this broker will listen on
         * @param port the port this broker will bind on
         * @return this
         * @throws IllegalStateException if the Zookeeper server has not been initialized
         */
        public KafkaClusterBuilder withBroker(int id, String host, int port) {
            if (zk == null) {
                throw new IllegalStateException("A Kafka broker needs a Zookeeper connection");
            }
            Path logDir = base.resolve("kafka").resolve(String.valueOf(id)).resolve("log");
            KafkaBroker broker = new KafkaBroker(logDir, id, zk.getConnectionString(), host, port);
            brokers.put(id, broker);
            return this;
        }

        /**
         * Modifies this newBuilder, attaching a new Kafka broker
         * @param id the broker id
         * @param host the hostname that this broker will listen on
         * @param port the port this broker will bind on
         * @param properties additional properties
         * @return this
         * @throws IllegalStateException if the Zookeeper server has not been initialized
         */
        public KafkaClusterBuilder withBroker(int id, String host, int port, Map<String, Object> properties) {
            if (zk == null) {
                throw new IllegalStateException("A Kafka broker needs a Zookeeper connection");
            }
            Path logDir = base.resolve("kafka").resolve(String.valueOf(id)).resolve("log");
            KafkaBroker broker = new KafkaBroker(logDir, id, zk.getConnectionString(), host, port, properties);
            brokers.put(id, broker);
            return this;
        }

        /**
         * Build a {@link KafkaCluster} instance
         * @return a new {@link KafkaCluster} instance
         * @throws IllegalStateException if a Zookeeper server has not been configured
         * @throws IllegalStateException if no Kafka brokers were specified
         */
        public KafkaCluster build() {
            if (zk == null) {
                throw new IllegalStateException("You cannot build a KafkaCluster without a Zookeeper server");
            }
            if (brokers.isEmpty()) {
                throw new IllegalStateException("You cannot build a KafkaCluster without Kafka brokers");
            }
            return new KafkaCluster(zk, brokers, base);
        }
    }

    private FileVisitor<Path> recursiveDeleteVisitor = new SimpleFileVisitor<Path>() {

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            FileVisitResult result = super.visitFile(file, attrs);
            if (result != FileVisitResult.CONTINUE) {
                return result;
            }
            Files.delete(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            FileVisitResult result = super.postVisitDirectory(dir, exc);
            if (result != FileVisitResult.CONTINUE) {
                return result;
            }
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
        }
    };
}
