package configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConsumerConfiguration {
    private static final String SERVER = "";

    public static Properties properties(){
        Properties properties = null;
        try (InputStream propertiesStream = new FileInputStream("src/main/resources/application.properties")) {
            properties = new Properties();
            properties.load(propertiesStream);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(0);
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("kafka.servers"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }
}
