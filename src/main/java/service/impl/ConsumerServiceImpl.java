package service.impl;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import service.ConsumerService;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

public class ConsumerServiceImpl implements ConsumerService {
    private final Logger LOGGER = Logger.getLogger(ConsumerServiceImpl.class.getName());
    private final String PATH = "src/main/resources/application.properties";


    @Override
    public Properties properties(String path) {
        Properties properties = null;
        try (InputStream propertiesStream = new FileInputStream(path)) {
            properties = new Properties();
            properties.load(propertiesStream);
        } catch (IOException ioException) {
            LOGGER.severe(ioException.getMessage());
            System.exit(0);
        }
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    @Override
    public Set<String> subscribe(Consumer<String, String> consumer, String topic) {
        consumer.subscribe(Collections.singleton(topic));
        return consumer.subscription();
    }

    @Override
    public Set<String> consume(Consumer<String, String> consumer) {
        ConsumerRecords<String, String> consumerRecords = null;
        try {
            LocalDateTime startTime = LocalDateTime.now();
            LocalDateTime endTime = startTime.plusMinutes(1);
            while (LocalDateTime.now().isBefore(endTime)) {
                consumerRecords = consumer.poll(Duration.ofMinutes(endTime.getMinute()));
                for (ConsumerRecord record : consumerRecords) {
                    LOGGER.info("topic: " + record.topic() + " | key: " + record.key() + " | value: " + record.value());
                }
            }
        } catch (WakeupException wakeupException) {
            LOGGER.severe(wakeupException.getMessage());
        } catch (RuntimeException runtimeException) {
            LOGGER.severe(runtimeException.getMessage());
        } finally {
            consumer.close();
        }
        return consumer.subscription();
    }

    public void start() {
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties(PATH));
        subscribe(consumer, properties(PATH).getProperty("topic"));
        consume(consumer);
    }
}
