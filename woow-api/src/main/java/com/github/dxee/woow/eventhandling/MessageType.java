package com.github.dxee.woow.eventhandling;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.protobuf.Message;

import java.lang.reflect.Type;

public final class MessageType {

    private final String typeName;

    public MessageType(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MessageType that = (MessageType) o;
        return Objects.equal(typeName, that.typeName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(typeName);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("typeName", typeName)
                .toString();
    }

    public static MessageType of(Message protoMessage) {
        return new MessageType(protoMessage.getClass().getTypeName());
    }

    public static MessageType of(Type t) {
        return new MessageType(t.getTypeName());
    }
}
