package service;

import org.apache.kafka.clients.consumer.Consumer;
import java.util.Properties;
import java.util.Set;

public interface ConsumerService {
    Properties properties(String path) throws Exception;
    Set<String> subscribe(Consumer<String, String> consumer, String topic) throws Exception;
    Set<String> consume(Consumer<String, String> consumer, String consumptionRuntime) throws Exception;
    void start() throws Exception;
}
