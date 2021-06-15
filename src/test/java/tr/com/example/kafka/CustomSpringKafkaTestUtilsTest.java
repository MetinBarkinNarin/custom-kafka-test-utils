package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class CustomSpringKafkaTestUtilsTest {
    final static String ANY_TOPIC = "topic-name";
    private final static Serde<String> ANY_KEY_SERDE = Serdes.String();
    private final static Serde<Long> ANY_VALUE_SERDE = Serdes.Long();

    @Mock
    private static EmbeddedKafkaBroker embeddedKafka;

    @Test
    void embeddedKafkaShouldNotBeNull() {
        assertThat(embeddedKafka).isNotNull();
    }

    @Test
    void constructConsumerSuccessfully() {
        Consumer<String, Long> consumer = constructConsumer();
        assertThat(consumer).isNotNull();
    }

    @ParameterizedTest(name = "{displayName} - [{index}] {arguments}")
    @MethodSource(value = "getConsumerNullArgs")
    void nullParameterToConsumerCausesNullPointerException(String topicName, Serde<?> keySerde, Serde<?> valueSerde, EmbeddedKafkaBroker embeddedKafka) {
        assertThrows(
                NullPointerException.class,
                () -> CustomSpringKafkaTestUtils.consumer(topicName, keySerde, valueSerde, embeddedKafka)
        );
    }

    @Test
    void constructProducerSuccessfully() {
        setUpEmbeddedKafka();
        KafkaTemplate<String, Long> producer = CustomSpringKafkaTestUtils.producer(ANY_KEY_SERDE, ANY_VALUE_SERDE, embeddedKafka);
        assertThat(producer).isNotNull();
    }

    @ParameterizedTest(name = "{displayName} - [{index}] {arguments}")
    @MethodSource(value = "getProducerNullArgs")
    void nullParameterToProducerCausesNullPointerException(Serde<?> keySerde, Serde<?> valueSerde, EmbeddedKafkaBroker embeddedKafka) {
        assertThrows(
                NullPointerException.class,
                () -> CustomSpringKafkaTestUtils.producer(keySerde, valueSerde, embeddedKafka)
        );
    }

    private static Stream<Arguments> getConsumerNullArgs() {
        setUpEmbeddedKafka();
        return Stream.of(
                Arguments.of(null, ANY_KEY_SERDE, ANY_VALUE_SERDE, embeddedKafka),
                Arguments.of(ANY_TOPIC, null, ANY_VALUE_SERDE, embeddedKafka),
                Arguments.of(ANY_TOPIC, ANY_KEY_SERDE, null, embeddedKafka),
                Arguments.of(ANY_TOPIC, ANY_KEY_SERDE, ANY_VALUE_SERDE, null)
        );
    }

    private static Stream<Arguments> getProducerNullArgs() {
        setUpEmbeddedKafka();
        return Stream.of(
                Arguments.of(null, ANY_VALUE_SERDE, embeddedKafka),
                Arguments.of(ANY_KEY_SERDE, null, embeddedKafka),
                Arguments.of(ANY_KEY_SERDE, ANY_VALUE_SERDE, null)
        );
    }

    private static void setUpEmbeddedKafka() {
        given(embeddedKafka.getBrokersAsString()).willReturn("http://localhost:9092");
    }

    private Consumer<String, Long> constructConsumer() {
        setUpEmbeddedKafka();
        return CustomSpringKafkaTestUtils.consumer(ANY_TOPIC, ANY_KEY_SERDE, ANY_VALUE_SERDE, embeddedKafka);
    }
}
