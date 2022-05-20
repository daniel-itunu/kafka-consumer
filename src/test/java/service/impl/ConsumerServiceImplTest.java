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

    /***
     * This method tests the connect method to ensure config properties were successfully and accurately loaded.
     */
    @Test
    void properties() throws Exception {
        Properties properties = consumerService.connect("src/main/resources/application.properties");
        assertTrue("localhost:9092".equals(properties.getProperty("bootstrap.servers")));
        assertTrue("gamers-group".equals(properties.getProperty("group.id")));
        assertTrue("gamers-topic".equals(properties.getProperty("topic")));
        assertTrue("latest".equals(properties.getProperty("auto.offset.reset")));
    }

    /***
     * This method tests the subscribe method to ensure accurate and successful subscription of consumer to topic.
     */
    @Test
    void subscribe() throws Exception {
        Set<String> setOfTopics = consumerService.subscribe(mockConsumer, "testing-subscription-to-topic");
        assertTrue(setOfTopics.size() == 1);
        assertEquals("testing-subscription-to-topic", setOfTopics.stream().collect(Collectors.toList()).get(0));
    }


    /***
     * This method tests the subscribe method against illegal arguments caused when a null/empty/blank value is passed as topic.
     */
    @Test
    void subscribeIllegalAccessException() {
        Exception illegalAccessException = assertThrows(Exception.class, () -> {
            mockConsumer.close();
            consumerService.subscribe(mockConsumer, "  ");
        });
        assertTrue("Topic collection to subscribe to cannot contain null or empty topic".equals(illegalAccessException.getMessage()));
    }

    /***
     * This method tests the consume method to ensure successful and accurate consumption under the time(minutes) specified
     * in application.properties file.
     */
    @Test
    void consume() throws Exception {
        mockConsumer.subscribe(Collections.singleton("gamers-topic"));
        mockConsumer.rebalance(Collections.singletonList(new TopicPartition("gamers-topic", 0)));
        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("gamers-topic", 0), 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
        mockConsumer.addRecord(new ConsumerRecord<String, String>("gamers-topic", 0, 0L, "key", "value"));
        Set<String> subscription = consumerService.consume(mockConsumer, "1");
        assertTrue("gamers-topic".equals(subscription.stream().collect(Collectors.toList()).get(0)));
    }


    /***
     * This method tests the consume method against runtime exceptions likely to occur during
     * consumption.
     */
    @Test
    void consumptionRuntimeException() {
        mockConsumer.subscribe(Collections.singleton("gamers-topic"));
        mockConsumer.close();
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            consumerService.consume(mockConsumer, "1");
        });
        assertTrue("This consumer has already been closed.".equals(exception.getMessage()));
    }


    /***
     * This method tests the consume method against number format exceptions likely to occur during
     * if a non-natural/numeric value is passed as value to the consume.time.minutes in application.properties file.
     */
    @Test
    void consumptionNumberFormatException() {
        mockConsumer.subscribe(Collections.singleton("gamers-topic"));
        NumberFormatException exception = assertThrows(NumberFormatException.class, () -> {
            consumerService.consume(mockConsumer, "");
        });
        assertTrue("For input string: \"\"".equals(exception.getMessage()));
    }

    /***
     * This method tests the connect method against IO exceptions likely to occur during
     * while reading config in application.properties file.
     */
    @Test
    void connectIOException() {
        Exception exception = assertThrows(Exception.class, () -> {
            consumerService.connect("/wrong/path");
        });
        assertTrue("/wrong/path (No such file or directory)".equals(exception.getMessage()));
    }


    /***
     * This method acts as an integration test to run all methods/processes like it would in real life.
     */
    @Test
    void start() {
        try {
            consumerService.start();
            assertTrue(true);
        } catch (Exception ex) {
            assertTrue(false);
        }
    }
}