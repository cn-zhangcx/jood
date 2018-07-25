package com.github.dxee.woow.messaging;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Strings;

/**
 * Naming scheme for messaging:
 * <p>
 * topic = "inbox" ["_" inbox_name ] "-" service_name
 * service_name = kafka_topic_char
 * inbox_name = kafka_topic_char
 * <p>
 * kafka_topic_char = "[a-zA-Z0-9\\._\\-]"        // letters, numbers, ".", "_", "-"
 */
public final class Topic {

    private final String topic;

    public Topic(String topicName) {
        this.topic = topicName;
    }

    public String topic() {
        return topic;
    }

    public boolean isEmpty() {
        return Strings.isNullOrEmpty(topic);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Topic topic1 = (Topic) o;
        return Objects.equal(topic, topic1.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topic);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("topic", topic)
                .toString();
    }
}
