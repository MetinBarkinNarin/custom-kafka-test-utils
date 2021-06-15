package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class CustomSpringKafkaTestUtils_CloseTest {
    @Mock
    private Consumer<String, Long> consumer;

    @Test
    void closeSuccessfully() {
        CustomSpringKafkaTestUtils.close(consumer);
        then(consumer).should(times(1)).close();
    }
}
