package service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.util.Properties;
import java.util.Set;

public interface ConsumerService {
    Properties properties(String path);
    Set<String> subscribe(Consumer<String, String> consumer, String topic);
    Set<String> consume(Consumer<String, String> consumer);
}
