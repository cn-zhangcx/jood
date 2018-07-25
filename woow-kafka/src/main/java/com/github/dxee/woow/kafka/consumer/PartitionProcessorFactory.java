package com.github.dxee.woow.kafka.consumer;

import com.github.dxee.woow.eventhandling.ErrorHandler;
import org.apache.kafka.common.TopicPartition;

public final class PartitionProcessorFactory {
    private final EventListenerMapping eventListenerMapping;
    private final ErrorHandler errorHandler;

    public PartitionProcessorFactory(EventListenerMapping eventListenerMapping, ErrorHandler errorHandler) {
        this.eventListenerMapping = eventListenerMapping;
        this.errorHandler = errorHandler;
    }

    public PartitionProcessor newProcessorFor(TopicPartition partitionKey) {
        return new PartitionProcessor(partitionKey, eventListenerMapping, errorHandler);
    }
}
