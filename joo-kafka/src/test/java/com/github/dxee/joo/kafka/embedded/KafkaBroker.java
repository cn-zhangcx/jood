package com.github.dxee.joo.kafka.embedded;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

class KafkaBroker {

    private final String listener;

    private final Map<String, Object> props;

    private KafkaServerStartable kafka;

    KafkaBroker(Path logDir, int brokerId, String zkConnect, String host, int port) {
        listener = String.format("%s:%d", host, port);
        props = new HashMap<>();
        props.put("zookeeper.connect", zkConnect);
        props.put("listeners", String.format("PLAINTEXT://%s", listener));
        props.put("log.dir", logDir.toString());
        props.put("broker.id", brokerId);
        props.put("offsets.topic.num.partitions", "1");
        props.put("auto.create.topics.enable", false);
        props.put("group.initial.rebalance.delay.ms", 0);
    }

    KafkaBroker(Path logDir, int brokerId, String zkConnect, String host, int port, Map<String, Object> properties) {
        this(logDir, brokerId, zkConnect, host, port);
        props.putAll(properties);
    }

    String getListener() {
        return listener;
    }

    void start(int brokers) {
        props.put("offsets.topic.replication.factor", (short) brokers);
        KafkaConfig config = new KafkaConfig(props);
        kafka = new KafkaServerStartable(config);
        kafka.startup();
    }

    void stop() {
        kafka.shutdown();
    }
}
