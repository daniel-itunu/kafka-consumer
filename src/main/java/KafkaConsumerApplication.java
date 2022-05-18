import service.impl.ConsumerServiceImpl;

public class KafkaConsumerApplication {

    public static void main(String[] args) {
        ConsumerServiceImpl consumerService = new ConsumerServiceImpl();
        consumerService.start();
    }
}
