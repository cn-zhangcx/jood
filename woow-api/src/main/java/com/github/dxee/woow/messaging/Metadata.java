package com.github.dxee.woow.messaging;

import com.github.dxee.woow.WoowContext;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;

/**
 * Metadata information about the Message such as
 * - If it was received from Kafka or is a newly created mesage.
 * - Kafka related information such as topic, partition id, partitioning key and the offset of the message in the
 * partition.
 * - Header fields sent with the Message (in the Envolope),
 * e.g. message id, type of the inner message, correlation ids, etc.
 * <p>
 * Depending on the message exchange pattern, some fields are optional.
 * <p>
 * For request-response, the request requires to have the reply-to topic set.
 * The consumer of the request must send the response back to the reply-to topic.
 * In the response, the requestCorrelationId is required and refers to the message id of the original request.
 */
public class Metadata {
    // Immutable

    // Keep in sync with Messaging.proto / message Envelope

    // Inbound or outbound message, i.e. did we receive it or is it a newly created one?
    private final boolean wasReceived;

    // Kafka information
    // The topic this message was received on (INBOUND) or is to be send to (OUTBOUND).
    private final Topic topic;
    // The key used to determine the partition in the topic. May be null.
    private final String partitioningKey;
    // The id of the topic partition - only for INBOUND.
    private final int partitionId;
    // The offset of the message - only for INBOUND
    private final long offset;

    // Message headers - see also Messaging.proto
    private final String messageId;


    // Tracing/correlation ids
    // OPTIONAL. A correlation id to correlate multiple messages,
    // rpc request/responses, etc. belonging to a trace/flow/user request/etc..
    private final String correlationId;

    // Message exchange patterns
    // -> request/reply
    // REQUIRED for RESPONSE. This response correlates to the message id of the original request.
    private final String requestCorrelationId;
    // REQUIRED for REQUEST. Send responses for this request to the given address. See class Topic for syntax.
    private final Topic replyTo;

    // REQUIRED.
    private final MessageType type;

    // Object instantiation is done via factory
    public Metadata(boolean wasReceived, Topic topic, String partitioningKey, int partitionId,
                    long offset, String messageId, String correlationId, String requestCorrelationId,
                    Topic replyTo, MessageType type) {
        this.wasReceived = wasReceived;

        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is required");
        }
        this.topic = topic;

        // null partitioningKey is ok, producer will select partition (round-robin)
        this.partitioningKey = partitioningKey;

        this.partitionId = partitionId;
        this.offset = offset;

        if (Strings.isNullOrEmpty(messageId)) {
            throw new IllegalArgumentException("non-empty messageId is required");
        }
        this.messageId = messageId;

        this.correlationId = correlationId;

        this.requestCorrelationId = requestCorrelationId;
        this.replyTo = replyTo;

        if (type == null) {
            throw new IllegalArgumentException("type is required");
        }
        this.type = type;
    }

    public boolean isInbound() {
        return wasReceived();
    }

    public boolean isOutbound() {
        return !isInbound();
    }

    public boolean wasReceived() {
        return wasReceived;
    }

    public Topic getTopic() {
        return topic;
    }

    public String getPartitioningKey() {
        return partitioningKey;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getOffset() {
        return offset;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getRequestCorrelationId() {
        return requestCorrelationId;
    }

    public Topic getReplyTo() {
        return replyTo;
    }

    public MessageType getType() {
        return type;
    }


    public WoowContext newContextFromMetadata() {
        return new WoowContext(correlationId);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("wasReceived", wasReceived)
                .add("topic", topic)
                .add("partitioningKey", partitioningKey)
                .add("partitionId", partitionId)
                .add("offset", offset)
                .add("messageId", messageId)
                .add("correlationId", correlationId)
                .add("requestCorrelationId", requestCorrelationId)
                .add("replyTo", replyTo)
                .add("type", type)
                .toString();
    }
}
