package com.github.dxee.woow.kafka.consumer;

import com.github.dxee.woow.messaging.TypeDictionary;
import org.apache.kafka.common.TopicPartition;

public final class PartitionProcessorFactory {
    private final TypeDictionary typeDictionary;
    private final FailedMessageProcessor failedMessageProcessor;

    public PartitionProcessorFactory(TypeDictionary typeDictionary, FailedMessageProcessor failedMessageProcessor) {
        this.typeDictionary = typeDictionary;
        this.failedMessageProcessor = failedMessageProcessor;
    }

    public PartitionProcessor newProcessorFor(TopicPartition partitionKey) {
        return new PartitionProcessor(partitionKey, typeDictionary, failedMessageProcessor);
    }
}
