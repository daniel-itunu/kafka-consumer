package service.impl;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerServiceImplTest {
    private ConsumerServiceImpl consumerService;
    private Consumer<String, String> mockConsumer;

    @BeforeEach
    void setUp() {
        consumerService = new ConsumerServiceImpl();
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    }

    @Test
    void properties() {
        Properties properties = consumerService.properties("src/main/resources/application.properties");
        assertTrue(properties.getProperty("bootstrap.servers").equalsIgnoreCase("localhost:9092"));
        assertTrue(properties.getProperty("group.id").equalsIgnoreCase("gamers-group"));
        assertTrue(properties.getProperty("topic").equalsIgnoreCase("gamers-topic"));
    }

    @Test
    void subscribe() {
        Set<String> setOfTopics= consumerService.subscribe(mockConsumer, "testing-subscription-to-topic");
        assertTrue(setOfTopics.size()==1);
        assertEquals("testing-subscription-to-topic", setOfTopics.stream().toArray()[0]);
    }

    @Test
    void consume() {
        mockConsumer.subscribe(Collections.singleton("gamers-topic"));
        Set<String> subscription= consumerService.consume(mockConsumer);
        assertTrue("gamers-topic".equals(subscription.stream().collect(Collectors.toList()).get(0)));
    }
}