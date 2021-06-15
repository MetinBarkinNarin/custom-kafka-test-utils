package tr.com.example.kafka;

import org.apache.kafka.streams.KeyValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

final class CityMother {
    private CityMother() { }

    final static City ADANA = new City("01", "Adana");
    final static City AMASYA = new City("05", "Amasya");
    final static City ANKARA = new City("06", "Ankara");
    final static City ANTALYA = new City("07", "Antalya");
    final static City BOLU = new City("14", "Bolu");
    final static City BURSA = new City("16", "Bursa");
    final static City ERZURUM = new City("25", "Erzurum");
    final static City KASTAMONU = new City("37", "Kastamonu");
    final static City MALATYA = new City("44", "Malatya");
    final static City ORDU = new City("52", "Ordu");
    final static City SAMSUN = new City("55", "Samsun");
    final static City TOKAT = new City("60", "Tokat");
    final static City ZONGULDAK = new City("67", "Zonguldak");

    final static List<City> EMPTY_CITY_LIST;
    final static List<KeyValue<String, City>> EMPTY_KEY_VALUE_CITY_LIST;
    final static List<City> CITY_LIST;
    final static List<KeyValue<String, City>> KEY_VALUE_CITY_LIST;

    static {
        EMPTY_CITY_LIST = Collections.emptyList();
        EMPTY_KEY_VALUE_CITY_LIST = Collections.emptyList();
        CITY_LIST = Arrays.asList( ADANA, AMASYA, ANKARA, ANTALYA, BOLU, BURSA, ERZURUM, KASTAMONU, MALATYA, ORDU, SAMSUN, ZONGULDAK );
        KEY_VALUE_CITY_LIST = CITY_LIST.stream().map(c -> new KeyValue<>(c.getId(), c)).collect(Collectors.toList());
    }

    static class City {
        private String id;
        private String name;
        public City() {}
        public City(String id, String name) {
            Objects.requireNonNull(id);
            Objects.requireNonNull(name);
            this.id = id; this.name = name;
        }
        public String getId() { return id; }
        public String getName() { return name; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof City)) return false;
            City city = (City) o;
            return Objects.equals(id, city.id) &&
                    Objects.equals(name, city.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }
    }
}
