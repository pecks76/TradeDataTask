package com.greg.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class TradeDataConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Nested Map: country -> {flowType -> totalTradeUSD}
    private final Map<String, Map<String, Double>> aggregates = new HashMap<>();

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "trade-consumer-group")
    public void consume(ConsumerRecord<String, String> record) {
        try {

            String value = record.value();
            System.out.println("Received message: " + value);

            JsonNode json = objectMapper.readTree(record.value());
            String country = json.get("country_or_area").asText();
            String flow = json.get("flow").asText();
            double tradeUsd = json.get("trade_usd").asDouble();

            aggregates
                    .computeIfAbsent(country, k -> new HashMap<>())
                    .merge(flow, tradeUsd, Double::sum);

        } catch (Exception e) {
            System.err.println("⚠️ Failed to process record: " + e.getMessage());
        }
    }

    public void printAggregates() {
        System.out.println("==== Aggregated Trade Data ====");
        for (var countryEntry : aggregates.entrySet()) {
            String country = countryEntry.getKey();
            System.out.println("Country: " + country);
            for (var flowEntry : countryEntry.getValue().entrySet()) {
                System.out.printf("  %s: %.2f USD%n", flowEntry.getKey(), flowEntry.getValue());
            }
        }
    }
}


