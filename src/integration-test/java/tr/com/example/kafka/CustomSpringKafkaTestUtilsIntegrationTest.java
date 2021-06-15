package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import tr.com.example.kafka.CityMother.City;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static tr.com.example.kafka.CityMother.CITY_LIST;
import static tr.com.example.kafka.CityMother.KEY_VALUE_CITY_LIST;

@SpringJUnitConfig
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(
        partitions = 1,
        topics = {
                CustomSpringKafkaTestUtilsIntegrationTest.CITY_TOPIC
        }
)
public class CustomSpringKafkaTestUtilsIntegrationTest {
    static final String CITY_TOPIC = "city-topic";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private KafkaTemplate<String, City> cityKafkaTemplate;

    private Consumer<String, City> cityConsumer;


    @BeforeEach
    void setUp() {
        cityKafkaTemplate = CustomSpringKafkaTestUtils.producer(Serdes.String(), new JsonSerde<>(City.class), embeddedKafka);
        cityConsumer = CustomSpringKafkaTestUtils.consumer(CITY_TOPIC, Serdes.String(), new JsonSerde<>(City.class), embeddedKafka);
    }

    @AfterEach
    void tearDown() {
        CustomSpringKafkaTestUtils.close(cityConsumer);
    }

    @Test
    void testTimeout() {
        Duration timeout = Duration.ofSeconds(11);
        try (ConfigurableApplicationContext ignored = startApplication()) {
            Instant now = Instant.now();
            List<KeyValue<String, City>> keyValueList = CustomSpringKafkaTestUtils.drainStreamOutput(cityConsumer, timeout);
            Instant later = Instant.now();
            Duration drainDuration = Duration.between(now, later);
            assertThat(drainDuration.compareTo(timeout)).isGreaterThanOrEqualTo(0);
            assertThat(keyValueList)
                    .isNotNull()
                    .hasSize(0);
        }
    }

    @Test
    void returnEmptyListWhenTopicHasNoRecords() {
        try (ConfigurableApplicationContext ignored = startApplication()) {
            Optional<Boolean> resultOptional = CustomSpringKafkaTestUtils.produceStreamSynchronously(cityKafkaTemplate, CITY_TOPIC, Stream.empty(), City::getId);
            List<KeyValue<String, City>> keyValueList = CustomSpringKafkaTestUtils.drainStreamOutput(cityConsumer, Duration.ofSeconds(10));
            assertThat(resultOptional)
                    .isEmpty();
            assertThat(keyValueList)
                    .isNotNull()
                    .hasSize(0);
        }
    }

    @Test
    void returnEmptyMapWhenTopicHasNoRecords() {
        try (ConfigurableApplicationContext ignored = startApplication()) {
            Optional<Boolean> resultOptional = CustomSpringKafkaTestUtils.produceStreamSynchronously(cityKafkaTemplate, CITY_TOPIC, Stream.empty(), City::getId);
            Map<String, City> cityMap = CustomSpringKafkaTestUtils.drainTableOutput(cityConsumer, Duration.ofSeconds(10));
            assertThat(resultOptional)
                    .isEmpty();
            assertThat(cityMap)
                    .isNotNull()
                    .hasSize(0);
        }
    }

    @Test
    void drainCityList() {
        try (ConfigurableApplicationContext ignored = startApplication()) {
            Optional<Boolean> resultOptional = CustomSpringKafkaTestUtils.produceStreamSynchronously(cityKafkaTemplate, CITY_TOPIC, CITY_LIST.stream(), City::getId);
            List<KeyValue<String, City>> keyValueList = CustomSpringKafkaTestUtils.drainStreamOutput(cityConsumer);
            assertThat(resultOptional)
                    .isPresent()
                    .hasValue( Boolean.TRUE );
            assertThat(keyValueList)
                    .isNotNull()
                    .isNotEmpty()
                    .hasSize(CITY_LIST.size())
                    .isEqualTo(KEY_VALUE_CITY_LIST);
        }
    }

    @Test
    void drainCityTable() {
        try (ConfigurableApplicationContext ignored = startApplication()) {
            Optional<Boolean> resultOptional = CustomSpringKafkaTestUtils.produceKeyValuesSynchronously(cityKafkaTemplate, CITY_TOPIC, KEY_VALUE_CITY_LIST.stream());
            Map<String, City> cityMap = CustomSpringKafkaTestUtils.drainTableOutput(cityConsumer);

            assertThat(resultOptional)
                    .isPresent()
                    .hasValue( Boolean.TRUE );
            assertThat(cityMap)
                    .isNotNull()
                    .isNotEmpty()
                    .hasSize(KEY_VALUE_CITY_LIST.size())
                    .isEqualToComparingFieldByFieldRecursively(
                            KEY_VALUE_CITY_LIST.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    kv -> kv.key,
                                                    kv -> kv.value
                                            )
                                    )
                    );
        }
    }

    private ConfigurableApplicationContext startApplication() {
        SpringApplication app = new SpringApplication(DummyApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        return app.run(
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--spring.cloud.stream.kafka.streams.binder.brokers=" + embeddedKafka.getBrokersAsString()
        );
    }

}
