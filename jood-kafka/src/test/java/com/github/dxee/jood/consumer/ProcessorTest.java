package com.github.dxee.jood.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ProcessorTest {

    @Mock
    private ConsumerRecordRelay<Integer, String> relay;

    private volatile AssertionError assertionError;

    private ConsumerRecord<Integer, String> record = new ConsumerRecord<>("testTopic",
            1, 42, 1234, "SomeValue");

    @Rule
    public TestName name = new TestName();

    @Before
    public void printTestHeader() {
        System.out.println("\n=======================================================");
        System.out.println("  Running Test : " + name.getMethodName());
        System.out.println("=======================================================\n");
    }

    @Test
    public void processMessage() throws Exception {
        AtomicInteger messageCounter = new AtomicInteger(0);

        PartitionProcessor<Integer, String> processor = new PartitionProcessor<>(
                new TopicPartition("Hello", 1), relay, message -> {
            try {
                messageCounter.incrementAndGet();
                assertThat(message.value(), is(equalTo("SomeValue")));
            } catch (AssertionError ex) {
                assertionError = ex;
            }
        }, 42);

        new Thread(processor).start();
        processor.queue(record);

        await().until(messageCounter::get, is(equalTo(1)));
        processor.stop();

        if (assertionError != null) {
            throw assertionError;
        }

        verify(relay, times(1)).setOffset(record);
    }

    @Test
    public void processMessage_with_exception() throws Exception {
        AtomicInteger messageCounter = new AtomicInteger(0);

        PartitionProcessor<Integer, String> processor = new PartitionProcessor<>(
                new TopicPartition("Hello", 1), relay, message -> {
            try {
                messageCounter.incrementAndGet();
                assertThat(message.value(), is(equalTo("SomeValue")));
                throw new RuntimeException("Foobar! Something went wrong");
            } catch (AssertionError ex) {
                assertionError = ex;
            }
        }, 42);

        new Thread(processor).start();
        processor.queue(record);

        await().until(messageCounter::get, is(equalTo(1)));

        processor.stop();
        processor.queue(record);

        if (assertionError != null) {
            throw assertionError;
        }

        verify(relay, never()).setOffset(record);
    }

    @Test
    public void stoppedSet_when_exception() throws Exception {
        final PartitionProcessor<Integer, String> processor = new PartitionProcessor<>(
                new TopicPartition("Hello", 1), relay, message -> {
            throw new RuntimeException("Foobar! Something went wrong");
        }, 42);

        new Thread(processor).start();
        processor.queue(record);

        await().until(processor::isStopped, is(equalTo(true)));
    }
}
