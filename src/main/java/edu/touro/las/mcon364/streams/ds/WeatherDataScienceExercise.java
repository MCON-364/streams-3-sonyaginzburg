package edu.touro.las.mcon364.streams.ds;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.stream.*;


public class WeatherDataScienceExercise {

    record WeatherRecord(
            String stationId,
            String city,
            String date,
            double temperatureC,
            int humidity,
            double precipitationMm
    ) {}

    public static void main(String[] args) throws Exception {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();

        System.out.println("Total raw rows (excluding header): " + (rows.size() - 1));
        System.out.println("Total cleaned rows: " + cleaned.size());

        // TODO 1:
        // Count how many valid weather records remain after cleaning.
        long count = cleaned.stream().count();

        // TODO 2:
        // Compute the average temperature across all valid rows.
        double avgTemp = cleaned.stream().mapToDouble(WeatherRecord::temperatureC).average().getAsDouble();

        // TODO 3:
        // Find the city with the highest average temperature.
        Map<String, Double> avgTempBYCity = cleaned.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city, Collectors.averagingDouble(WeatherRecord::temperatureC)));
        String hottestCity = avgTempBYCity.entrySet().stream()
                .max(Comparator.comparingDouble(Map.Entry::getValue)).get().getKey();

        // TODO 4:
        // Group records by city.
        Map<String, List<WeatherRecord>> byCity = cleaned.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city, Collectors.toList()));

        // TODO 5:
        // Compute average precipitation by city.
        Map<String, Double> avgP = cleaned.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city, Collectors.averagingDouble(WeatherRecord::precipitationMm)));

        // TODO 6:
        // Partition rows into freezing days (temperature <= 0)
        // and non-freezing days (temperature > 0).
        Map<Boolean, List<WeatherRecord>> partition = cleaned.stream()
                .collect(Collectors.partitioningBy(r -> r.temperatureC() <= 0 ));

        // TODO 7:
        // Create a Set<String> of all distinct cities.
        Set<String> cities = cleaned.stream()
                .map(WeatherRecord::city)
                .collect(Collectors.toSet());


        // TODO 8:
        // Find the wettest single day.
        WeatherRecord wettestDay = cleaned.stream()
                .max(Comparator.comparingDouble(WeatherRecord::precipitationMm))
                .orElseThrow();

        // TODO 9:
        // Create a Map<String, Double> from city to average humidity.
        Map<String, Double> avgHumidity = cleaned.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city, Collectors.averagingDouble(WeatherRecord::humidity)));

        // TODO 10:
        // Produce a list of formatted strings like:
        // "Miami on 2025-01-02: 25.1C, humidity 82%"
        List<String> formatted = cleaned.stream()
                .map(r -> String.format("%s on %s: %.1fC, humidity %d%%", r.city(), r.date(), r.temperatureC(), r.humidity()))
                .toList();

        // TODO 11 (optional):
        // Build a Map<String, CityWeatherSummary> for all cities.
        Map<String, CityWeatherSummary> summary = cleaned.stream()
                .collect(Collectors.groupingBy(
                        r -> r.city(),
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                records -> new CityWeatherSummary(
                                        records.get(0).city(),
                                        records.stream().count(),
                                        records.stream().mapToDouble(WeatherRecord::temperatureC).average().getAsDouble(),
                                        records.stream().mapToDouble(WeatherRecord::precipitationMm).average().getAsDouble(),
                                        records.stream().mapToDouble(WeatherRecord::temperatureC).max().getAsDouble()
                                )
                        )
                ));
        // Put your code below these comments or refactor into helper methods.

    }

    static Optional<WeatherRecord> parseRow(String row) {
        // 1. Split the row by commas
        String[] parts =  row.split(",");
        // 2. Reject malformed rows
        if (parts.length != 6)
            return Optional.empty();
        // 3. Reject rows with missing temperature
        if (parts[3].isBlank()) {
            return Optional.empty();
        }
        // 4. Parse numeric values safely       stationId,city,date,temperatureC,humidity,precipitationMm
        try {
            String stationId = parts[0].trim();
            String city = parts[1].trim();
            String date = parts[2].trim();
            double temperatureC = Double.parseDouble(parts[3]);
            int humidity = Integer.parseInt(parts[4]);
            double precipitationMm = Double.parseDouble(parts[5]);
            return Optional.of(new WeatherRecord(
                    stationId,
                    city,
                    date,
                    temperatureC,
                    humidity,
                    precipitationMm
            ));
        } catch (NumberFormatException e) {
            // 5. Return Optional.empty() if parsing fails
            return Optional.empty();
        }
    }

    static boolean isValid(WeatherRecord r) {
        // Keep only rows where:
        // - temperature is between -60 and 60
        return (r.temperatureC() >= -60 && r.temperatureC() <= 60)
        // - humidity is between 0 and 100
        && (r.humidity() >= 0 && r.humidity() <= 100)
        // - precipitation is >= 0
        && (r.precipitationMm() >= 0);
    }

    record CityWeatherSummary(
            String city,
            long dayCount,
            double avgTemp,
            double avgPrecipitation,
            double maxTemp
    ) {}

    private static List<String> readCsvRows(String fileName) throws IOException {
        InputStream in = WeatherDataScienceExercise.class.getResourceAsStream(fileName);
        if (in == null) {
            throw new NoSuchFileException("Classpath resource not found: " + fileName);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }

    static List<WeatherRecord> buildCleanedList() throws Exception {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");
        return rows.stream()
                .skip(1)
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();
    }



}
