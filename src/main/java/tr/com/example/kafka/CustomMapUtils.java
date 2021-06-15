package tr.com.example.kafka;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public final class CustomMapUtils {

    private CustomMapUtils() {
    }

    /**
     * Creates a map entry (for use with {@link CustomMapUtils#mkMap(Map.Entry[])})
     *
     * @param k   The key
     * @param v   The value
     * @param <K> The key type
     * @param <V> The value type
     * @return An entry
     */
    public static <K, V> Map.Entry<K, V> mkEntry(final K k, final V v) {
        return new Map.Entry<K, V>() {
            @Override
            public K getKey() {
                return k;
            }

            @Override
            public V getValue() {
                return v;
            }

            @Override
            public V setValue(final V value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Creates a map from a sequence of entries
     *
     * @param entries The entries to map
     * @param <K>     The key type
     * @param <V>     The value type
     * @return A map
     */
    @SafeVarargs
    public static <K, V> Map<K, V> mkMap(final Map.Entry<K, V>... entries) {
        return Arrays
                .stream(entries)
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (v1, v2) -> v2
                        )
                );
    }
}
