package tr.com.example.kafka.contract;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.contract.verifier.messaging.kafka.KafkaStubMessagesInitializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import tr.com.example.kafka.CustomSpringKafkaTestUtils;

import java.util.Map;
import java.util.stream.Collectors;

public class CustomContractVerifierKafkaStubMessagesInitializer implements KafkaStubMessagesInitializer {

    public CustomContractVerifierKafkaStubMessagesInitializer() {
    }

    public Map<String, Consumer> initialize(EmbeddedKafkaBroker broker, KafkaProperties kafkaProperties) {
        return broker.getTopics().stream()
                .collect(Collectors.toMap(
                        topic -> topic,
                        topic -> CustomSpringKafkaTestUtils.consumer(
                                topic,
                                kafkaProperties.getConsumer().getKeyDeserializer(),
                                kafkaProperties.getConsumer().getValueDeserializer(),
                                broker
                        ))
                );
    }
}
