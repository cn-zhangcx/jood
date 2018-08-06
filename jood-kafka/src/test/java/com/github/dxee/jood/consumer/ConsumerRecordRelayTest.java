package com.github.dxee.jood.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerRecordRelayTest {

    @Mock
    private Consumer<Integer, String> consumer;

    @Mock
    private KafConsumer<Integer, String> kafConsumer;

    private ConsumerRecord<Integer, String> record = new ConsumerRecord<>("testTopic", 1, 42, 1234, "SomeValue");

    @Rule
    public TestName name = new TestName();

    @Before
    public void printTestHeader() {
        System.out.println("\n=======================================================");
        System.out.println("  Running Test : " + name.getMethodName());
        System.out.println("=======================================================\n");
    }

    @Test
    public void relay_and_commitOffset() throws Exception {
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> map = new HashMap<>();
        map.put(new TopicPartition("testTopic", 1), Arrays.asList(record));
        ConsumerRecords<Integer, String> records = new ConsumerRecords<>(map);

        when(consumer.poll(any(Duration.class))).thenReturn(records);

        ConsumerRecordRelay<Integer, String> relay = new ConsumerRecordRelay<>(consumer, kafConsumer);
        relay.setOffset(record);
        new Thread(relay).start();

        verify(kafConsumer, timeout(1000).atLeastOnce()).relay(eq(record));
        verify(consumer, timeout(1000).atLeastOnce()).commitAsync(any(), any());
        relay.stop();
    }

    @Test
    public void relay_with_exception() throws Exception {
        when(consumer.poll(any(Duration.class))).thenThrow(RuntimeException.class);

        ConsumerRecordRelay<Integer, String> relay = new ConsumerRecordRelay<>(consumer, kafConsumer);
        new Thread(relay).start();
        verify(kafConsumer, never()).relay(record);
        verify(consumer, timeout(1000).only()).poll(any(Duration.class));
        relay.stop();
    }

}