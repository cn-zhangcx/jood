package com.github.dxee.woow.messaging;

import com.google.common.base.MoreObjects;
import com.google.protobuf.Message;

public final class EventMessage<T extends Message> {
    private final T payload;
    private final Metadata metadata;

    public EventMessage(T payload, Metadata metadata) {
        this.payload = payload;
        this.metadata = metadata;
    }

    public T getPayload() {
        return payload;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("payload", payload)
                .add("metadata", metadata)
                .toString();
    }
}