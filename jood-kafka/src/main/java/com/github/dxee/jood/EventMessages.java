package com.github.dxee.jood;

import com.github.dxee.jood.eventhandling.EventMessage;
import com.github.dxee.jood.eventhandling.MetaData;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.UUID;

/**
 * Message translate
 *
 * @author bing.fan
 * 2018-07-06 18:15
 */
public final class EventMessages {
    private EventMessages() {
        // Prevent instantiation.
    }

    public static EventMessage<? extends Message> oneWayMessage(String target, String partitionKey,
                                                                Message protoPayloadMessage, JoodContext context) {
        boolean wasReceived = false;

        String messageId = UUID.randomUUID().toString();
        String correlationId = context.getCorrelationId();

        // not required
        String replyTo = null;
        // not required
        String requestCorrelationId = "";

        String type = TypeNames.of(protoPayloadMessage);

        MetaData meta = new MetaData(wasReceived, target, partitionKey, -1, -1, messageId,
                correlationId, requestCorrelationId, replyTo, type);
        return new EventMessage<>(protoPayloadMessage, meta);
    }

    public static EventMessage<? extends Message> requestFor(String target, String replyTo, String partitionKey,
                                                             Message protoPayloadMessage, JoodContext context) {
        boolean wasReceived = false;

        String messageId = UUID.randomUUID().toString();
        String correlationId = context.getCorrelationId();

        String type = TypeNames.of(protoPayloadMessage);

        // Use default inbox for service.
        if (replyTo == null) {
            throw new IllegalArgumentException("replyTo required");
        }

        // not required
        String requestCorrelationId = "";

        MetaData meta = new MetaData(wasReceived, target, partitionKey, -1, -1, messageId,
                correlationId, requestCorrelationId, replyTo, type);
        return new EventMessage<>(protoPayloadMessage, meta);
    }


    public static EventMessage<? extends Message> replyTo(EventMessage originalRequest,
                                                                              Message protoPayloadMessage,
                                                                              JoodContext context) {
        boolean wasReceived = false;

        // By default, return to sender topic using same partitioning scheme.
        String target = originalRequest.getMetaData().getReplyTo();
        String partitionKey = originalRequest.getMetaData().getPartitioningKey();

        String messageId = UUID.randomUUID().toString();
        String correlationId = context.getCorrelationId();

        String requestCorrelationId = originalRequest.getMetaData().getMessageId();

        // not required
        String replyTo = null;

        String type = TypeNames.of(protoPayloadMessage);

        MetaData meta = new MetaData(wasReceived, target, partitionKey, -1, -1, messageId,
                correlationId, requestCorrelationId, replyTo, type);
        return new EventMessage<>(protoPayloadMessage, meta);
    }


    public static EventMessage<? extends Message> fromKafka(Message protoMessage, Envelope envelope,
                                                            ConsumerRecord<String, byte[]> record) {
        boolean wasReceived = true;

        String topic = record.topic();
        String partitioningKey = record.key();
        int partitionId = record.partition();
        long offset = record.offset();

        String messageId = envelope.getMessageId();
        String correlationId = envelope.getCorrelationId();

        String type = TypeNames.of(protoMessage);

        String requestCorrelationId = envelope.getRequestCorrelationId();
        String replyTo = envelope.getReplyTo();

        MetaData meta = new MetaData(wasReceived, topic, partitioningKey, partitionId, offset, messageId,
                correlationId, requestCorrelationId, replyTo, type);
        return new EventMessage<>(protoMessage, meta);
    }


    public static Envelope toKafka(EventMessage message) {
        Envelope.Builder envelope = Envelope.newBuilder();
        MetaData meta = message.getMetaData();

        envelope.setMessageId(meta.getMessageId());

        // Correlation ids are set when building the message
        if (!Strings.isNullOrEmpty(meta.getCorrelationId())) {
            envelope.setCorrelationId(meta.getCorrelationId());
        }

        // Message exchange pattern headers
        if (meta.getReplyTo() != null) {
            envelope.setReplyTo(meta.getReplyTo());
        }
        if (!Strings.isNullOrEmpty(meta.getRequestCorrelationId())) {
            envelope.setRequestCorrelationId(meta.getRequestCorrelationId());
        }

        // Payload (mandatory fields!)
        envelope.setTypeName(meta.getTypeName());
        // Serialize the proto payload to bytes
        envelope.setInnerMessage(message.getPayload().toByteString());

        return envelope.build();
    }
}
