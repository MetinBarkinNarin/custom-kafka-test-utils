package tr.com.example.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class CustomSpringKafkaTestUtils_SenderMethodsTest {
    final static String ANY_TOPIC = "topic-name";

    @Mock
    private KafkaTemplate<String, CityMother.City> kafkaTemplate;

    @Mock(lenient = true)
    private ListenableFuture<SendResult<String, CityMother.City>> successfulFuture;

    @Mock(lenient = true)
    private ListenableFuture<SendResult<String, CityMother.City>> unsuccessfulFuture;

    @Captor
    private ArgumentCaptor<String> stringArgumentCaptor;

    @BeforeEach
    void setUp() {
        CompletableFuture<SendResult<String, CityMother.City>> successfulSendResult = new CompletableFuture<>();
        successfulSendResult.complete(new SendResult<>(null, null));

        CompletableFuture<SendResult<String, CityMother.City>> unsuccessfulSendResult = new CompletableFuture<>();
        unsuccessfulSendResult.completeExceptionally(new NullPointerException());

        given(successfulFuture.completable()).willReturn(successfulSendResult);
        given(unsuccessfulFuture.completable()).willReturn(unsuccessfulSendResult);
    }

    @Test
    void sendEmptyValueStream() {
        Optional<Boolean> resultOptional = CustomSpringKafkaTestUtils.produceStreamSynchronously(kafkaTemplate, ANY_TOPIC, CityMother.EMPTY_CITY_LIST.stream(), CityMother.City::getId);
        assertsForEmptyStream(kafkaTemplate, resultOptional);
    }

    @Test
    void sendValueStreamSuccessfully() {
        attemptToSend(
                successfulFuture,
                Boolean.TRUE,
                CityMother.CITY_LIST.size(),
                () -> CustomSpringKafkaTestUtils.produceStreamSynchronously(kafkaTemplate, ANY_TOPIC, CityMother.CITY_LIST.stream(), CityMother.City::getId)
        );
    }

    @Test
    void sendValueStreamUnsuccessfully() {
        attemptToSend(
                unsuccessfulFuture,
                Boolean.FALSE,
                CityMother.CITY_LIST.size(),
                () -> CustomSpringKafkaTestUtils.produceStreamSynchronously(kafkaTemplate, ANY_TOPIC, CityMother.CITY_LIST.stream(), CityMother.City::getId)
        );
    }

    @Test
    void sendEmptyKeyValueStream() {
        Optional<Boolean> resultOptional = CustomSpringKafkaTestUtils.produceKeyValuesSynchronously(kafkaTemplate, ANY_TOPIC, CityMother.EMPTY_KEY_VALUE_CITY_LIST.stream());
        assertsForEmptyStream(kafkaTemplate, resultOptional);
    }

    @Test
    void sendKeyValueStreamSuccessfully() {
        attemptToSend(
                successfulFuture,
                Boolean.TRUE,
                CityMother.KEY_VALUE_CITY_LIST.size(),
                () -> CustomSpringKafkaTestUtils.produceKeyValuesSynchronously(kafkaTemplate, ANY_TOPIC, CityMother.KEY_VALUE_CITY_LIST.stream())
        );
    }

    @Test
    void sendKeyValueStreamUnsuccessfully() {
        attemptToSend(
                unsuccessfulFuture,
                Boolean.FALSE,
                CityMother.KEY_VALUE_CITY_LIST.size(),
                () -> CustomSpringKafkaTestUtils.produceKeyValuesSynchronously(kafkaTemplate, ANY_TOPIC, CityMother.KEY_VALUE_CITY_LIST.stream())
        );
    }

    private void assertsForEmptyStream(KafkaTemplate<String, CityMother.City> kafkaTemplate, Optional<Boolean> resultOptional) {
        assertThat(resultOptional).isEmpty();
        then(kafkaTemplate).should(times(0)).send(anyString(), anyString(), any(CityMother.City.class));
    }

    private void attemptToSend(ListenableFuture<SendResult<String, CityMother.City>> future,
                               Boolean expectedResult,
                               int times,
                               Supplier<Optional<Boolean>> senderSupplier) {
        given(kafkaTemplate.send(stringArgumentCaptor.capture(), anyString(), any(CityMother.City.class))).willReturn( future );
        Optional<Boolean> resultOptional = senderSupplier.get();

        assertThat(resultOptional)
                .isPresent()
                .hasValue( expectedResult );
        assertThat(stringArgumentCaptor.getValue())
                .isEqualTo(ANY_TOPIC);
        then(kafkaTemplate)
                .should(times(times))
                .send(anyString(), anyString(), any(CityMother.City.class));
    }
}
