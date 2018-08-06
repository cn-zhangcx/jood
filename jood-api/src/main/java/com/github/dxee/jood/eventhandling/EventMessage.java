package com.github.dxee.jood.eventhandling;

import com.google.common.base.MoreObjects;
import com.google.protobuf.Message;

/**
 * Event message
 *
 * @author bing.fan
 * 2018-07-05 17:29
 */
public final class EventMessage<T extends Message> {
    private final T payload;
    private final MetaData metaData;

    public EventMessage(T payload, MetaData metaData) {
        this.payload = payload;
        this.metaData = metaData;
    }

    public T getPayload() {
        return payload;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("payload", payload)
                .add("metaData", metaData)
                .toString();
    }
}