package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class CustomSpringKafkaTestUtils_DrainersTest {
    private final static String ANY_TOPIC_NAME = "topic";
    private final static int ANY_PARTITION = 0;

    @Mock
    private Consumer<String, CityMother.City> consumer;

    private ConsumerRecords<String, CityMother.City> consumerRecords;

    @BeforeEach
    void setUp() {
        int[] offset = { 0 };
        List<ConsumerRecord<String, CityMother.City>> consumerRecordList = CityMother.CITY_LIST
                .stream()
                .map(c -> new ConsumerRecord<>(ANY_TOPIC_NAME, ANY_PARTITION, offset[0]++, c.getId(), c))
                .collect(Collectors.toList());
        Map<TopicPartition, List<ConsumerRecord<String, CityMother.City>>> map = new HashMap<>();
        map.put(new TopicPartition(ANY_TOPIC_NAME, ANY_PARTITION), consumerRecordList);
        this.consumerRecords = new ConsumerRecords<>(map);
    }

    @Test
    void returnEmptyListWhenTopicHasNoRecords() {
        given(consumer.poll(any(Duration.class))).willReturn(new ConsumerRecords<>(new HashMap<>()));

        List<KeyValue<String, CityMother.City>> keyValueList = CustomSpringKafkaTestUtils.drainStreamOutput(consumer, Duration.ofSeconds(10));
        assertThat(keyValueList)
                .isNotNull()
                .hasSize(0);
    }

    @Test
    void returnEmptyMapWhenTopicHasNoRecords() {
        given(consumer.poll(any(Duration.class))).willReturn(new ConsumerRecords<>(new HashMap<>()));

        Map<String, CityMother.City> cityMap = CustomSpringKafkaTestUtils.drainTableOutput(consumer, Duration.ofSeconds(10));
        assertThat(cityMap)
                .isNotNull()
                .hasSize(0);
    }

    @Test
    void drainCityTable() {
        given(consumer.poll(any(Duration.class))).willReturn(consumerRecords);

        Map<String, CityMother.City> cityMap = CustomSpringKafkaTestUtils.drainTableOutput(consumer, Duration.ofSeconds(10));
        assertThat(cityMap)
                .isNotNull()
                .hasSize(consumerRecords.count())
                .isEqualToComparingFieldByFieldRecursively(
                        CityMother.CITY_LIST.stream().collect(Collectors.toMap(CityMother.City::getId, Function.identity()))
                );
    }

    @Test
    void drainCityList() {
        given(consumer.poll(any(Duration.class))).willReturn(consumerRecords);
        List<KeyValue<String, CityMother.City>> cityList = CustomSpringKafkaTestUtils.drainStreamOutput(consumer);
        assertThat(cityList)
                .isNotNull()
                .hasSize(consumerRecords.count())
                .isEqualTo(
                        CityMother.KEY_VALUE_CITY_LIST
                );
    }

}
