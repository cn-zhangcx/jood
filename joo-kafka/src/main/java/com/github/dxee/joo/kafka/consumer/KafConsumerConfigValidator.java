package com.github.dxee.joo.kafka.consumer;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

/**
 * Do some validation for the kafka consumer config.
 *
 * @author bing.fan
 * 2018-08-02 14:54
 */
final class KafConsumerConfigValidator {
    static final String NULL_PROPS_MESSAGE = "Kafka properties must not be null";
    static final String AUTO_COMMIT_ENABLED_MSG =
            String.format("%s must not be enabled for the KafConsumer", ENABLE_AUTO_COMMIT_CONFIG);

    private KafConsumerConfigValidator() {
    }

    static Properties validate(Properties props) {
        verifyPropsNotNull(props);
        verifyAutoCommitDisabled(props);
        return props;
    }

    private static void verifyPropsNotNull(Properties props) {
        if (props == null) {
            throw new IllegalArgumentException(NULL_PROPS_MESSAGE);
        }
    }

    private static void verifyAutoCommitDisabled(Properties props) {
        Boolean isAutoCommitEnabled = Boolean.parseBoolean(props.getProperty(ENABLE_AUTO_COMMIT_CONFIG));
        if (isAutoCommitEnabled) {
            throw new IllegalArgumentException(AUTO_COMMIT_ENABLED_MSG);
        }
    }
}
