package tr.com.example.kafka;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Map.Entry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static tr.com.example.kafka.CustomMapUtils.mkEntry;
import static tr.com.example.kafka.CustomMapUtils.mkMap;

class CustomMapUtilsTest {

    @Test
    void constructEntry() {
        Entry<String, CityMother.City> entry = mkEntry(CityMother.ADANA.getId(), CityMother.ADANA);

        assertThat(entry)
                .isNotNull()
                .satisfies(e -> {
                    assertThat(e.getKey()).isEqualTo(CityMother.ADANA.getId());
                    assertThat(e.getValue()).isEqualTo(CityMother.ADANA);
                });
    }

    @Test
    void doNotAllowToUpdateValue() {
        assertThrows(UnsupportedOperationException.class, () -> {
            Entry<String, CityMother.City> entry = mkEntry(CityMother.ADANA.getId(), CityMother.ADANA);
            entry.setValue( CityMother.ANKARA );
        });
    }

    @Test
    void constructMap() {
        Map<String, CityMother.City> map = mkMap(
                mkEntry(CityMother.ADANA.getId(), CityMother.ADANA),
                mkEntry(CityMother.ANKARA.getId(), CityMother.ANKARA),
                mkEntry(CityMother.ANTALYA.getId(), CityMother.ANTALYA)
        );

        assertThat(map)
                .isNotNull()
                .isNotEmpty()
                .hasSize(3)
                .containsEntry(CityMother.ADANA.getId(), CityMother.ADANA)
                .containsEntry(CityMother.ANKARA.getId(), CityMother.ANKARA)
                .containsEntry(CityMother.ANTALYA.getId(), CityMother.ANTALYA);
    }

    @Test
    void constructEmptyMap() {
        Map<String, CityMother.City> emptyMap = mkMap( );

        assertThat(emptyMap)
                .isNotNull()
                .matches(Map::isEmpty);
    }

}
