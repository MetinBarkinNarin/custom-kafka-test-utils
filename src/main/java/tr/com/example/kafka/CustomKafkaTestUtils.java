/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tr.com.example.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.test.TestUtils;

import java.nio.file.DirectoryNotEmptyException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Utility functions to make integration testing more convenient.
 */
public class CustomKafkaTestUtils {

    public static <K, V> KStream<K, V> toStream(StreamsBuilder streamsBuilder, String topicName, Serde<K> keySerde, Serde<V> valueSerde) {
        return streamsBuilder.stream(topicName, Consumed.with(keySerde, valueSerde));
    }

    public static <K, V> KTable<K, V> toTable(StreamsBuilder streamsBuilder, String topicName, Serde<K> keySerde, Serde<V> valueSerde) {
        return streamsBuilder.table(topicName, Consumed.with(keySerde, valueSerde));
    }

    public static TopologyTestDriver topologyTestDriver(StreamsBuilder streamsBuilder, Properties properties) {
        return new TopologyTestDriver(streamsBuilder.build(), properties);
    }

    public static void close(TopologyTestDriver topologyTestDriver) throws Exception {
        if (topologyTestDriver != null)
            try {
                topologyTestDriver.close();
            } catch (Exception e) {
                throwIfNotDirectoryNotEmptyException(e);
            }
    }

    /**
     * Asserts that the key-value store contains exactly the expected content and nothing more.
     *
     * @param store    the store to be validated
     * @param expected the expected contents of the store
     * @param <K>      the store's key type
     * @param <V>      the store's value type
     */
    public static <K, V> void assertThatKeyValueStoreContainsExactly(final ReadOnlyKeyValueStore<K, V> store, final Map<K, V> expected)
            throws InterruptedException {
        TestUtils.waitForCondition(() -> {
                    boolean result = expected.keySet()
                            .stream()
                            .allMatch(k -> expected.get(k).equals(store.get(k)));
                    return result && expected.size() == sizeOf(store);
                },
                30000,
                "Expected values not found in KV store");
    }

    /**
     * Returns up to `maxMessages` message-values from the topic.
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer configuration
     * @param maxMessages    Maximum number of messages to read via the consumer.
     * @return The values retrieved via the consumer.
     */
    public static <K, V> List<V> readValues(final String topic, final Properties consumerConfig, final int maxMessages) {
        return IntegrationTestUtils.readValues(topic, consumerConfig, maxMessages);
    }

    /**
     * Returns as many messages as possible from the topic until a (currently hardcoded) timeout is
     * reached.
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer configuration
     * @return The KeyValue elements retrieved via the consumer.
     */
    public static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic, final Properties consumerConfig) {
        return IntegrationTestUtils.readKeyValues(topic, consumerConfig);
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from are
     * already configured in the consumer).
     *
     * @param topic          Kafka topic to read messages from
     * @param consumerConfig Kafka consumer configuration
     * @param maxMessages    Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    public static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic, final Properties consumerConfig, final int maxMessages) {
        return IntegrationTestUtils.readKeyValues(topic, consumerConfig, maxMessages);
    }

    /**
     * Write a collection of KeyValueWithTimestamp pairs, with explicitly defined timestamps, to Kafka
     * and wait until the writes are acknowledged.
     *
     * @param topic          Kafka topic to write the data records to
     * @param records        Data records to write to Kafka
     * @param producerConfig Kafka producer configuration
     * @param <K>            Key type of the data records
     * @param <V>            Value type of the data records
     */
    public static <K, V> void produceKeyValuesWithTimestampsSynchronously(
            final String topic,
            final Collection<KeyValueWithTimestamp<K, V>> records,
            final Properties producerConfig)
            throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesWithTimestampsSynchronously(topic, records, producerConfig);
    }

    /**
     * @param topic          Kafka topic to write the data records to
     * @param records        Data records to write to Kafka
     * @param producerConfig Kafka producer configuration
     * @param <K>            Key type of the data records
     * @param <V>            Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(
            final String topic,
            final Collection<KeyValue<K, V>> records,
            final Properties producerConfig)
            throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronously(topic, records, producerConfig);
    }

    public static <V> void produceValuesSynchronously(
            final String topic, final Collection<V> records, final Properties producerConfig)
            throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceValuesSynchronously(topic, records, producerConfig);
    }

    /**
     * Like {@link CustomKafkaTestUtils#produceValuesSynchronously(String, Collection, Properties)}, except for use with
     * TopologyTestDriver tests, rather than "native" Kafka broker tests.
     *
     * @param topic              Kafka topic to write the data records to
     * @param values             Message values to write to Kafka (keys will have type byte[] and be set to null)
     * @param topologyTestDriver The {@link TopologyTestDriver} to send the data records to
     * @param valueSerializer    The {@link Serializer} corresponding to the value type
     * @param <V>                Value type of the data records
     */
    static <K, V> void produceValuesSynchronously(final String topic,
                                                  final List<V> values,
                                                  final TopologyTestDriver topologyTestDriver,
                                                  final Serializer<V> valueSerializer) {
        IntegrationTestUtils.produceValuesSynchronously(topic, values, topologyTestDriver, valueSerializer);
    }

    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(
            final Properties consumerConfig,
            final String topic,
            final int expectedNumRecords)
            throws InterruptedException {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, topic, expectedNumRecords);
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound in waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError if the given wait time elapses
     */
    public static <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(final Properties consumerConfig,
                                                                                  final String topic,
                                                                                  final int expectedNumRecords,
                                                                                  final long waitTime) throws InterruptedException {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, topic, expectedNumRecords, waitTime);
    }

    public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                                final String topic,
                                                                final int expectedNumRecords) throws InterruptedException {

        return IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfig, topic, expectedNumRecords);
    }

    /**
     * Wait until enough data (value records) has been consumed.
     *
     * @param consumerConfig     Kafka Consumer configuration
     * @param topic              Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @param waitTime           Upper bound in waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError if the given wait time elapses
     */
    public static <V> List<V> waitUntilMinValuesRecordsReceived(final Properties consumerConfig,
                                                                final String topic,
                                                                final int expectedNumRecords,
                                                                final long waitTime) throws InterruptedException {
        return IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerConfig, topic, expectedNumRecords, waitTime);
    }

    /**
     * Waits until the named store is queryable and, once it is, returns a reference to the store.
     * <p>
     * Caveat: This is a point in time view and it may change due to partition reassignment.
     * That is, the returned store may still not be queryable in case a rebalancing is happening or
     * happened around the same time.  This caveat is acceptable for testing purposes when only a
     * single `KafkaStreams` instance of the application is running.
     *
     * @param streams            the `KafkaStreams` instance to which the store belongs
     * @param storeName          the name of the store
     * @param queryableStoreType the type of the (queryable) store
     * @param <T>                the type of the (queryable) store
     * @return the same store, which is now ready for querying (but see caveat above)
     */
    public static <T> T waitUntilStoreIsQueryable(final String storeName,
                                                  final QueryableStoreType<T> queryableStoreType,
                                                  final KafkaStreams streams) throws InterruptedException {
        return IntegrationTestUtils.waitUntilStoreIsQueryable(storeName, queryableStoreType, streams);
    }

    public static <K, V> void assertThatKeyValueStoreContains(final ReadOnlyKeyValueStore<K, V> store,
                                                              final Map<K, V> expected)
            throws InterruptedException {
        IntegrationTestUtils.assertThatKeyValueStoreContains(store, expected);
    }

    /**
     * Asserts that the oldest available window in the window store contains the expected content.
     *
     * @param store    the store to be validated
     * @param expected the expected contents of the store
     * @param <K>      the store's key type
     * @param <V>      the store's value type
     */
    public static <K, V> void assertThatOldestWindowContains(final ReadOnlyWindowStore<K, V> store,
                                                             final Map<K, V> expected)
            throws InterruptedException {
        IntegrationTestUtils.assertThatOldestWindowContains(store, expected);
    }

    /**
     * Similar to {@link CustomKafkaTestUtils#waitUntilMinKeyValueRecordsReceived(Properties, String, int)}, except for
     * use with {@link TopologyTestDriver} tests. Because the test driver is synchronous, we don't need to poll for
     * the expected number of records, and then hope that these are all the results from our test. Instead, we can
     * just read out <em>all</em> the processing results, for use with deterministic validations.
     * <p>
     * Since this call is specifically for a table, we collect the observed records into a {@link Map} from keys to values,
     * representing the latest observed value for each key.
     *
     * @param topic              Topic to consume from
     * @param topologyTestDriver The {@link TopologyTestDriver} to read the data records from
     * @param keyDeserializer    The {@link Deserializer} corresponding to the key type
     * @param valueDeserializer  The {@link Deserializer} corresponding to the value type
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     * @return A {@link Map} representing the table constructed from the output topic.
     */
    public static <K, V> Map<K, V> drainTableOutput(final String topic,
                                                    final TopologyTestDriver topologyTestDriver,
                                                    final Deserializer<K> keyDeserializer,
                                                    final Deserializer<V> valueDeserializer) {
        return IntegrationTestUtils.drainTableOutput(topic, topologyTestDriver, keyDeserializer, valueDeserializer);
    }

    /**
     * Similar to {@link CustomKafkaTestUtils#waitUntilMinKeyValueRecordsReceived(Properties, String, int)}, except for
     * use with {@link TopologyTestDriver} tests. Because the test driver is synchronous, we don't need to poll for
     * the expected number of records, and then hope that these are all the results from our test. Instead, we can
     * just read out <em>all</em> the processing results, for use with deterministic validations.
     *
     * @param topic              Topic to consume from
     * @param topologyTestDriver The {@link TopologyTestDriver} to read the data records from
     * @param keyDeserializer    The {@link Deserializer} corresponding to the key type
     * @param valueDeserializer  The {@link Deserializer} corresponding to the value type
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     * @return A {@link List} of {@link KeyValue} pairs of results from the output topic, in the order they were produced.
     */
    public static <K, V> List<KeyValue<K, V>> drainStreamOutput(final String topic,
                                                                final TopologyTestDriver topologyTestDriver,
                                                                final Deserializer<K> keyDeserializer,
                                                                final Deserializer<V> valueDeserializer) {
        return IntegrationTestUtils.drainStreamOutput(topic, topologyTestDriver, keyDeserializer, valueDeserializer);
    }

    /**
     * Like {@link CustomKafkaTestUtils#produceKeyValuesSynchronously(String, Collection, Properties)}, except for use
     * with TopologyTestDriver tests, rather than "native" Kafka broker tests.
     *
     * @param topic              Kafka topic to write the data records to
     * @param records            Data records to write to Kafka
     * @param topologyTestDriver The {@link TopologyTestDriver} to send the data records to
     * @param keySerializer      The {@link Serializer} corresponding to the key type
     * @param valueSerializer    The {@link Serializer} corresponding to the value type
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(final String topic,
                                                            final List<KeyValue<K, V>> records,
                                                            final TopologyTestDriver topologyTestDriver,
                                                            final Serializer<K> keySerializer,
                                                            final Serializer<V> valueSerializer) {
        IntegrationTestUtils.produceKeyValuesSynchronously(topic, records, topologyTestDriver, keySerializer, valueSerializer);
    }

    /**
     * Like {@link CustomKafkaTestUtils#produceKeyValuesSynchronously(String, Collection, Properties)}, except for use
     * with TopologyTestDriver tests, rather than "native" Kafka broker tests.
     *
     * @param topic              Kafka topic to write the data records to
     * @param records            Data records to write to Kafka
     * @param topologyTestDriver The {@link TopologyTestDriver} to send the data records to
     * @param keySerializer      The {@link Serializer} corresponding to the key type
     * @param valueSerializer    The {@link Serializer} corresponding to the value type
     * @param timestamp          The timestamp to use for the produced records
     * @param <K>                Key type of the data records
     * @param <V>                Value type of the data records
     */
    public static <K, V> void produceKeyValuesSynchronously(final String topic,
                                                            final List<KeyValue<K, V>> records,
                                                            final TopologyTestDriver topologyTestDriver,
                                                            final Serializer<K> keySerializer,
                                                            final Serializer<V> valueSerializer,
                                                            final long timestamp) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
                topic,
                records,
                topologyTestDriver,
                keySerializer,
                valueSerializer,
                timestamp
        );
    }

    private static <K, V> int sizeOf(ReadOnlyKeyValueStore<K, V> store) {
        int[] size = {0};
        store.all().forEachRemaining(i -> size[0]++);
        return size[0];
    }

    private static void throwIfNotDirectoryNotEmptyException(Exception e) throws Exception {
        if (!(e.getCause() instanceof DirectoryNotEmptyException)) {
            throw e;
        } else {
            System.out.println("Ignoring exception, test failing in Windows due this exception: " + e.getLocalizedMessage());
        }
    }

    /**
     * A Serializer/Deserializer/Serde implementation for use when you know the data is always null
     *
     * @param <T> The type of the stream (you can parameterize this with any type,
     *            since we throw an exception if you attempt to use it with non-null data)
     */
    static class NothingSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {

        @Override
        public void configure(final Map<String, ?> configuration, final boolean isKey) {

        }

        @Override
        public T deserialize(final String topic, final byte[] bytes) {
            if (bytes != null) {
                throw new IllegalArgumentException("Expected [" + Arrays.toString(bytes) + "] to be null.");
            } else {
                return null;
            }
        }

        @Override
        public byte[] serialize(final String topic, final T data) {
            if (data != null) {
                throw new IllegalArgumentException("Expected [" + data + "] to be null.");
            } else {
                return null;
            }
        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<T> serializer() {
            return this;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this;
        }
    }
}

