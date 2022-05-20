package service.impl;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import service.ConsumerService;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

public class ConsumerServiceImpl implements ConsumerService {
    public static final String PATH = "src/main/resources/application.properties";
    private final Logger LOGGER = Logger.getLogger(ConsumerServiceImpl.class.getName());

    /**
     * This method provides entry point to running the consumer application
     */
    @Override
    public void start() throws Exception {
        KafkaConsumer<String, String> consumer;
        try {
            consumer = new KafkaConsumer<String, String>(connect(PATH));
        } catch (Exception ex) {
            LOGGER.info("Ensure all key-value pairs config in application.properties are present");
            LOGGER.severe("Exception occurred: " + ex.getCause().getMessage());
            throw new KafkaException(ex.getCause().getMessage());
        }
        subscribe(consumer, connect(PATH).getProperty("topic"));
        consume(consumer, connect(PATH).getProperty("consume.time.minutes"));
        consumer.close();
        LOGGER.info("Shutdown success");
    }


    /**
     * This method loads broker's key-value config from application.properties file.
     *
     * @param path: specifies the location of the application.properties file containing broker configurations.
     * @return: returns the properties loaded in the application.properties and ConsumerConfig(key-value deserializer)
     **/
    @Override
    public Properties connect(String path) throws Exception {
        Properties properties;
        try (InputStream propertiesStream = new FileInputStream(path)) {
            properties = new Properties();
            properties.load(propertiesStream);
        } catch (IOException ex) {
            LOGGER.info("Failed to load broker config in application.properties file");
            LOGGER.severe("Exception occurred: " + ex.getMessage());
            throw new IOException(ex.getMessage());
        }
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        LOGGER.info("Successfully listening to broker");
        return properties;
    }


    /**
     * This method subscribes the consumer to a topic.
     *
     * @param consumer: specifies the consumer object to subscribe to a topic.
     * @param topic:    specifies the topic to be subscribed to by the above consumer.
     * @return :specifies a set of subscriptions made by the consumer.
     **/
    @Override
    public Set<String> subscribe(Consumer<String, String> consumer, String topic) throws Exception {
        try {
            consumer.subscribe(Collections.singleton(topic));
        } catch (Exception ex) {
            consumer.close();
            LOGGER.info("Failed to subscribe to topic. Ensure topic is present in application.properties");
            LOGGER.severe("Exception occurred: topic cannot be null or empty");
            throw new Exception("Topic collection to subscribe to cannot contain null or empty topic");
        }
        LOGGER.info("Successfully subscribed to topic");
        return consumer.subscription();
    }

    /**
     * This method performs the consume operation in multithreaded fashion for a given minute specified in param(consumptionRuntime).
     *
     * @param consumer:           specifies the consumer object making the consume operation.
     * @param consumptionRuntime: specifies the amount of time in minutes the consume operation should run before graceful shutdown.
     * @return :specifies a set of subscriptions made by the consumer.
     */
    @Override
    public Set<String> consume(Consumer<String, String> consumer, String consumptionRuntime) {
        ConsumerRecords<String, String> consumerRecords;
        String statusMessage = "";
        Integer consumptionRuntimeIntValue;
        try {
            consumptionRuntimeIntValue = Integer.parseInt(consumptionRuntime);
        } catch (NumberFormatException ex) {
            LOGGER.info("consume.time.minutes value in application.properties cannot be null, empty or non numeric character");
            LOGGER.severe("Exception occurred: " + ex.getMessage());
            throw new NumberFormatException(ex.getMessage());
        }
        try {
            LocalTime endTime = LocalTime.now().plusMinutes(consumptionRuntimeIntValue);
            while (LocalTime.now().isBefore(endTime)) {
                consumerRecords = consumer.poll(Duration.ofSeconds(5));
                ConsumerRecords<String, String> finalConsumerRecords = consumerRecords;
                Runnable runnable = () -> {
                    for (ConsumerRecord record : finalConsumerRecords) {
                        LOGGER.info("topic: " + record.topic() + " | key: " + record.key() + " | value: " + record.value());
                    }
                };
                new Thread(runnable).start();
            }
        } catch (RuntimeException ex) {
            consumer.close();
            statusMessage = "Failed to poll records";
            LOGGER.info(statusMessage);
            LOGGER.severe("Exception occurred: " + ex.getMessage());
            throw new RuntimeException(ex.getMessage());
        }
        statusMessage = "Successfully poll records and closed consumer";
        LOGGER.info(statusMessage);
        return consumer.subscription();
    }

}
