package edu.touro.las.mcon364.streams.ds;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class WeatherDataScienceExerciseTest {

    @Test
    void parseRow_returnsRecord() {
        String row = "ST01,New York,2025-01-01,3.3,61,3.2";
        Optional<WeatherDataScienceExercise.WeatherRecord> result = WeatherDataScienceExercise.parseRow(row);
        assertTrue(result.isPresent());
        assertEquals("ST01", result.get().stationId());
        assertEquals("New York", result.get().city());
        assertEquals("2025-01-01", result.get().date());
        assertEquals(3.3, result.get().temperatureC());
        assertEquals(61, result.get().humidity());
        assertEquals(3.2, result.get().precipitationMm());
    }

    @Test
    void parseRow_tooFewColumns_returnsEmpty() {
        String row = "ST01,New York,2025-01-01";
        Optional<WeatherDataScienceExercise.WeatherRecord> result = WeatherDataScienceExercise.parseRow(row);
        assertTrue(result.isEmpty());
    }

    @Test
    void parseRow_missingTemperature_returnsEmpty() {
        String row = "ST01,New York,2025-01-01, ,61,3.2";
        Optional<WeatherDataScienceExercise.WeatherRecord> result = WeatherDataScienceExercise.parseRow(row);
        assertTrue(result.isEmpty());
    }

    @Test
    void parseRow_nonNumericTemperature_returnsEmpty() {
        String row = "ST01,New York,2025-01-01,sg,61,3.2";
        Optional<WeatherDataScienceExercise.WeatherRecord> result = WeatherDataScienceExercise.parseRow(row);
        assertTrue(result.isEmpty());
    }
    // 4 temperature tests
    @Test
    void isValid_Temperature60() {
        WeatherDataScienceExercise.WeatherRecord r = new WeatherDataScienceExercise.WeatherRecord("ST01", "New York", "2025-01-01", 60, 61, 3.2);
        assertTrue(WeatherDataScienceExercise.isValid(r));
    }

    @Test
    void isValid_TemperatureNeg60() {
        WeatherDataScienceExercise.WeatherRecord r = new WeatherDataScienceExercise.WeatherRecord("ST01", "New York", "2025-01-01", -60, 61, 3.2);
        assertTrue(WeatherDataScienceExercise.isValid(r));
    }
    @Test
    void isInValid_Temperature61() {
        WeatherDataScienceExercise.WeatherRecord r = new WeatherDataScienceExercise.WeatherRecord("ST01", "New York", "2025-01-01", 61, 61, 3.2);
        assertFalse(WeatherDataScienceExercise.isValid(r));
    }
    @Test
    void isInValid_TemperatureNeg61() {
        WeatherDataScienceExercise.WeatherRecord r = new WeatherDataScienceExercise.WeatherRecord("ST01", "New York", "2025-01-01", -61, 61, 3.2);
        assertFalse(WeatherDataScienceExercise.isValid(r));
    }

    // 4 humidity tests
    @Test
    void isValid_Humidity0() {
        WeatherDataScienceExercise.WeatherRecord r = new WeatherDataScienceExercise.WeatherRecord("ST01", "New York", "2025-01-01", 60, 0, 3.2);
        assertTrue(WeatherDataScienceExercise.isValid(r));
    }

    @Test
    void isValid_Humidity100() {
        WeatherDataScienceExercise.WeatherRecord r = new WeatherDataScienceExercise.WeatherRecord("ST01", "New York", "2025-01-01", 60, 100, 3.2);
        assertTrue(WeatherDataScienceExercise.isValid(r));
    }

    @Test
    void isInValid_HumidityNeg1() {
        WeatherDataScienceExercise.WeatherRecord r = new WeatherDataScienceExercise.WeatherRecord("ST01", "New York", "2025-01-01", 60, -1, 3.2);
        assertFalse(WeatherDataScienceExercise.isValid(r));
    }

    @Test
    void isInValid_Humidity101() {
        WeatherDataScienceExercise.WeatherRecord r = new WeatherDataScienceExercise.WeatherRecord("ST01", "New York", "2025-01-01", 60, 101, 3.2);
        assertFalse(WeatherDataScienceExercise.isValid(r));
    }

    // 2 precipitation
    @Test
    void isInValid_PrecipitationNeg() {
        WeatherDataScienceExercise.WeatherRecord r = new WeatherDataScienceExercise.WeatherRecord("ST01", "New York", "2025-01-01", 60, -1, -3.2);
        assertFalse(WeatherDataScienceExercise.isValid(r));
    }
    @Test
    void isValid_Precipitation0() {
        WeatherDataScienceExercise.WeatherRecord r = new WeatherDataScienceExercise.WeatherRecord("ST01", "New York", "2025-01-01", 60, 50, 0);
        assertTrue(WeatherDataScienceExercise.isValid(r));
    }
    //Integration tests, had a hard time with these and used AI

    @Test
    void integration_cleanedListIsNonEmpty() throws  Exception {
        List<WeatherDataScienceExercise.WeatherRecord> cleaned =
                WeatherDataScienceExercise.buildCleanedList();
        assertFalse(cleaned.isEmpty());
    }

    @Test
    void integration_allRecordsPassIsValid() throws  Exception {
        List<WeatherDataScienceExercise.WeatherRecord> cleaned =
                WeatherDataScienceExercise.buildCleanedList();
        assertTrue(cleaned.stream().allMatch(WeatherDataScienceExercise::isValid));
    }

    @Test
    void integration_hottestCityIsNonNull() throws  Exception {
        List<WeatherDataScienceExercise.WeatherRecord> cleaned =
                WeatherDataScienceExercise.buildCleanedList();
        // build avgTempByCity then find the max
        // assert the result is not null and not blank
        Map<String, Double> avgTempByCity = cleaned.stream()
                .collect(Collectors.groupingBy(WeatherDataScienceExercise.WeatherRecord::city, Collectors.averagingDouble(WeatherDataScienceExercise.WeatherRecord::temperatureC)));
        String hottestCity = avgTempByCity.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .get()
                .getKey();
        assertNotNull(hottestCity);
        assertFalse(hottestCity.isBlank());
    }

    @Test
    void integration_wettestDayPrecipitationNonNegative() throws  Exception {
        List<WeatherDataScienceExercise.WeatherRecord> cleaned =
                WeatherDataScienceExercise.buildCleanedList();
        WeatherDataScienceExercise.WeatherRecord wettestDay = cleaned.stream()
                .max(Comparator.comparingDouble(WeatherDataScienceExercise.WeatherRecord::precipitationMm))
                .orElseThrow();
        assertTrue(wettestDay.precipitationMm() >= 0);
    }


}