package service.impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

class ConsumerServiceImplTest {
    private ConsumerServiceImpl consumerService;
    private MockConsumer<String, String> mockConsumer;

    @BeforeEach
    void setUp() {
        consumerService = new ConsumerServiceImpl();
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    }

    @AfterEach
    void setDown() {
        mockConsumer.close();
    }

    @Test
    void properties() throws Exception {
        Properties properties = consumerService.connect("src/main/resources/application.properties");
        assertTrue("localhost:9092".equals(properties.getProperty("bootstrap.servers")));
        assertTrue("gamers-group".equals(properties.getProperty("group.id")));
        assertTrue("gamers-topic".equals(properties.getProperty("topic")));
    }

    @Test
    void subscribe() throws Exception {
        Set<String> setOfTopics= consumerService.subscribe(mockConsumer, "testing-subscription-to-topic");
        assertTrue(setOfTopics.size()==1);
        assertEquals("testing-subscription-to-topic", setOfTopics.stream().collect(Collectors.toList()).get(0));
    }

    @Test
    void subscribeIllegalAccessException(){
        Exception illegalAccessException = assertThrows(Exception.class, () -> {
            mockConsumer.close();
            consumerService.subscribe(mockConsumer, "  ");
        });
        assertTrue("Topic collection to subscribe to cannot contain null or empty topic".equals(illegalAccessException.getMessage()));
    }

    @Test
    void consume() throws Exception {
        mockConsumer.subscribe(Collections.singleton("gamers-topic"));
        mockConsumer.rebalance(Collections.singletonList(new TopicPartition("gamers-topic", 0)));
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("gamers-topic", 0), 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.addRecord(new ConsumerRecord<String, String>("gamers-topic", 0, 0L, "key", "value"));
        Set<String> subscription= consumerService.consume(mockConsumer, "1");
        assertTrue("gamers-topic".equals(subscription.stream().collect(Collectors.toList()).get(0)));
    }


    @Test
    void consumptionRuntimeException() {
        mockConsumer.subscribe(Collections.singleton("gamers-topic"));
        mockConsumer.close();
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            consumerService.consume(mockConsumer, "1");
        });
        assertTrue("This consumer has already been closed.".equals(exception.getMessage()));
    }

    @Test
    void consumptionNumberFormatException() {
        mockConsumer.subscribe(Collections.singleton("gamers-topic"));
        NumberFormatException exception = assertThrows(NumberFormatException.class, () -> {
            consumerService.consume(mockConsumer, "");
        });
        assertTrue("For input string: \"\"".equals(exception.getMessage()));
    }

    @Test
    void propertiesIOException() {
        Exception exception = assertThrows(Exception.class, () -> {
            consumerService.connect("/wrong/path");
        });
        assertTrue("/wrong/path (No such file or directory)".equals(exception.getMessage()));
    }
}