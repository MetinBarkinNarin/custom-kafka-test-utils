package tr.com.example.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KeyValue;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CustomSpringKafkaTestUtils {
    public static <K, V> Consumer<K, V> consumer(String topic,
                                                 Serde<K> keySerde,
                                                 Serde<V> valueSerde,
                                                 EmbeddedKafkaBroker embeddedKafka) {
        return consumer(
                topic,
                keySerde.deserializer(),
                valueSerde.deserializer(),
                createConsumerProps(embeddedKafka.getBrokersAsString())
        );
    }

    public static <K, V> Consumer<K, V> consumer(String topic,
                                                 Class<?> keyDeserializerClass,
                                                 Class<?> valueDeserializerClass,
                                                 EmbeddedKafkaBroker embeddedKafka) {
        return consumer(
                topic,
                null,
                null,
                createConsumerProps(embeddedKafka.getBrokersAsString(), keyDeserializerClass, valueDeserializerClass)
        );
    }

    public static <K, V> Consumer<K, V> consumer(String topic,
                                                 Serde<K> keySerde,
                                                 Serde<V> valueSerde,
                                                 String bootStrapServer) {
        return consumer(
                topic,
                keySerde.deserializer(),
                valueSerde.deserializer(),
                createConsumerProps(bootStrapServer)
        );
    }

    public static <K, V> Consumer<K, V> consumer(String topic,
                                                 Deserializer<K> keyDeserializer,
                                                 Deserializer<V> valueDeserializer,
                                                 EmbeddedKafkaBroker embeddedKafka) {
        return consumer(
                topic,
                keyDeserializer,
                valueDeserializer,
                createConsumerProps(embeddedKafka.getBrokersAsString())
        );
    }

    public static <K, V> Consumer<K, V> consumer(String topic,
                                                 Map<String, Object> consumerProps) {
        return consumer(topic, null, null, consumerProps);
    }

    public static <K, V> Consumer<K, V> consumer(String topic,
                                                 Deserializer<K> keyDeserializer,
                                                 Deserializer<V> valueDeserializer,
                                                 Map<String, Object> consumerProps
    ) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(consumerProps);

        DefaultKafkaConsumerFactory<K, V> kafkaConsumerFactory = Optional.ofNullable(keyDeserializer)
                .map(deserializer -> new DefaultKafkaConsumerFactory<>(consumerProps, deserializer, valueDeserializer))
                .orElse(new DefaultKafkaConsumerFactory<>(consumerProps));
        Consumer<K, V> consumer = kafkaConsumerFactory.createConsumer();
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    private static Map<String, Object> createConsumerProps(String bootStrapServer) {
        Map<String, Object> consumerProps = consumerProps(bootStrapServer, UUID.randomUUID().toString(), "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerProps;
    }

    private static Map<String, Object> createConsumerProps(String bootStrapServer, Class<?> keyDeserializer, Class<?> valueDeserializer) {
        Map<String, Object> consumerProps = consumerProps(bootStrapServer, UUID.randomUUID().toString(), "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        return consumerProps;
    }

    public static <K, V> Consumer<Object, Object> consumer(String topic,
                                                           Serde<K> keySerde,
                                                           Class<V> valuedeserializer,
                                                           String bootStrapServer,
                                                           String schemaRegistryUrl
    ) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(keySerde);
        Objects.requireNonNull(valuedeserializer);


        Map<String, Object> consumerProps = consumerProps(bootStrapServer, UUID.randomUUID().toString(), "false");
        consumerProps.put("value.deserializer", valuedeserializer);

        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("schema.registry.url", schemaRegistryUrl);
        Consumer<Object, Object> consumer = new DefaultKafkaConsumerFactory<>(consumerProps).createConsumer();

        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }


    public static <K, V> KafkaTemplate<K, V> producer(Serde<K> keySerde,
                                                      Serde<V> valueSerde,
                                                      EmbeddedKafkaBroker embeddedKafka) {
        Objects.requireNonNull(keySerde);
        Objects.requireNonNull(valueSerde);
        Objects.requireNonNull(embeddedKafka);

        Map<String, Object> senderPropsProduct = KafkaTestUtils.producerProps(embeddedKafka);
        senderPropsProduct.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerde.serializer().getClass());
        senderPropsProduct.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerde.serializer().getClass());
        senderPropsProduct.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        DefaultKafkaProducerFactory<K, V> pfMessage = new DefaultKafkaProducerFactory<>(senderPropsProduct);
        return new KafkaTemplate<>(pfMessage, true);
    }

    public static <K, V> KafkaTemplate<K, V> producer(Serde<K> keySerde,
                                                      Serde<V> valueSerde,
                                                      String bootStrapServer) {
        Objects.requireNonNull(keySerde);
        Objects.requireNonNull(valueSerde);
        Objects.requireNonNull(bootStrapServer);

        Map<String, Object> senderPropsProduct = senderProps(bootStrapServer);
        senderPropsProduct.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerde.serializer().getClass());
        senderPropsProduct.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerde.serializer().getClass());
        senderPropsProduct.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        DefaultKafkaProducerFactory<K, V> pfMessage = new DefaultKafkaProducerFactory<>(senderPropsProduct);
        return new KafkaTemplate<>(pfMessage, true);
    }

    public static Map<String, Object> senderProps(String brokers) {
        Map<String, Object> props = new HashMap();
        props.put("bootstrap.servers", brokers);
        props.put("retries", 0);
        props.put("batch.size", "16384");
        props.put("linger.ms", 1);
        props.put("buffer.memory", "33554432");
        props.put("key.serializer", IntegerSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        return props;
    }

    public static Map<String, Object> consumerProps(String brokers, String group, String autoCommit) {
        Map<String, Object> props = new HashMap();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", group);
        props.put("enable.auto.commit", autoCommit);
        props.put("auto.commit.interval.ms", "10");
        props.put("session.timeout.ms", "60000");
        props.put("key.deserializer", IntegerDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        return props;
    }

    public static <K, V> Optional<Boolean> produceStreamSynchronously(KafkaTemplate<K, V> kafkaTemplate,
                                                                      String topicName,
                                                                      Stream<V> stream,
                                                                      Function<V, K> idGetter) {
        return produceKeyValuesSynchronously(kafkaTemplate, topicName, stream.map(v -> toKeyValue(v, idGetter)));
    }

    public static <K, V> Optional<Boolean> produceKeyValuesSynchronously(KafkaTemplate<K, V> kafkaTemplate,
                                                                         String topicName,
                                                                         Stream<KeyValue<K, V>> stream) {
        return produceKeyValuesSynchronously(stream, o -> send(kafkaTemplate, topicName, o));
    }

    private static <K, V> Optional<Boolean> produceKeyValuesSynchronously(Stream<KeyValue<K, V>> stream,
                                                                          Function<KeyValue<K, V>, ListenableFuture<SendResult<K, V>>> sender) {
        return collectResult(stream.map(sender));
    }

    private static <K, V> ListenableFuture<SendResult<K, V>> send(KafkaTemplate<K, V> kafkaTemplate, String topicName, KeyValue<K, V> keyValue) {
        return kafkaTemplate.send(topicName, keyValue.key, keyValue.value);
    }

    private static <K, V> KeyValue<K, V> toKeyValue(V v, Function<V, K> idGetter) {
        return new KeyValue<>(idGetter.apply(v), v);
    }

    static <K, V> Optional<Boolean> collectResult(Stream<ListenableFuture<SendResult<K, V>>> stream) {
        return Optional
                .ofNullable(stream)
                .orElse(Stream.empty())
                .map(ListenableFuture::completable)
                .map(cf -> cf.handle((kvSendResult, throwable) -> throwable == null))
                .map(CompletableFuture::join)
                .reduce(Boolean::logicalAnd);
    }

    public static void close(Consumer consumer) {
        if (Objects.nonNull(consumer))
            consumer.close();
    }

    public static <K, V> Map<K, V> drainTableOutput(Consumer<K, V> consumer) {
        return drainTableOutput(consumer, Duration.ofMinutes(1));
    }

    public static <K, V> Map<K, V> drainTableOutput(Consumer<K, V> consumer, Duration timeout) {
        Stream<ConsumerRecord<K, V>> stream = drainer(consumer, timeout);
        return toMap(stream);
    }

    public static <K, V> List<KeyValue<K, V>> drainStreamOutput(Consumer<K, V> consumer) {
        return drainStreamOutput(consumer, Duration.ofMinutes(1));
    }

    public static <K, V> List<KeyValue<K, V>> drainStreamOutput(Consumer<K, V> consumer, Duration timeout) {
        Stream<ConsumerRecord<K, V>> stream = drainer(consumer, timeout);
        return toList(stream);
    }

    private static <K, V> Map<K, V> toMap(Stream<ConsumerRecord<K, V>> stream) {
        return stream
                .collect(
                        Collectors.toMap(
                                ConsumerRecord::key,
                                ConsumerRecord::value,
                                (v1, v2) -> v2
                        )
                );
    }

    private static <K, V> List<KeyValue<K, V>> toList(Stream<ConsumerRecord<K, V>> stream) {
        return stream
                .map(r -> new KeyValue<>(r.key(), r.value()))
                .collect(Collectors.toList());
    }

    static <K, V> Stream<ConsumerRecord<K, V>> drainer(Consumer<K, V> consumer, Duration timeout) {
        ConsumerRecords<K, V> records = KafkaTestUtils.getRecords(consumer, timeout.getSeconds() * 1_000);
        return StreamSupport.stream(records.spliterator(), false);
    }
}
