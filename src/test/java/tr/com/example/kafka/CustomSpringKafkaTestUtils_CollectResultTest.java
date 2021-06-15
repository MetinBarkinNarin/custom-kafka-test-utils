package tr.com.example.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class CustomSpringKafkaTestUtils_CollectResultTest {

    @Mock(lenient = true)
    private ListenableFuture<SendResult<String, Long>> successfulFuture;

    @Mock(lenient = true)
    private ListenableFuture<SendResult<String, Long>> unsuccessfulFuture;

    @BeforeEach
    void setUp() {
        CompletableFuture<SendResult<String, Long>> successfulSendResult = new CompletableFuture<>();
        successfulSendResult.complete(new SendResult<>(null, null));

        CompletableFuture<SendResult<String, Long>> unsuccessfulSendResult = new CompletableFuture<>();
        unsuccessfulSendResult.completeExceptionally(new NullPointerException());

        given(successfulFuture.completable()).willReturn(successfulSendResult);
        given(unsuccessfulFuture.completable()).willReturn(unsuccessfulSendResult);
    }

    @Test
    void returnEmptyOptionalForNullInput() {
        Optional<Boolean> resultOptional = CustomSpringKafkaTestUtils.collectResult(null);
        assertThat(resultOptional).isEmpty();
    }

    @Test
    void returnEmptyOptionalForEmptyStream() {
        Optional<Boolean> resultOptional = CustomSpringKafkaTestUtils.collectResult(Stream.empty());
        assertThat(resultOptional).isEmpty();
    }

    @Test
    void returnTrueWhenAllSentSuccessfully() {
        Optional<Boolean> resultOptional = CustomSpringKafkaTestUtils.collectResult(Stream.of(successfulFuture));
        assertThat(resultOptional)
                .isPresent()
                .hasValue(Boolean.TRUE);
    }

    @Test
    void returnFalseWhenAnyoneCouldNotBeSent() {
        Optional<Boolean> resultOptional = CustomSpringKafkaTestUtils.collectResult(Stream.of(successfulFuture, unsuccessfulFuture));
        assertThat(resultOptional)
                .isPresent()
                .hasValue(Boolean.FALSE);
    }
}
