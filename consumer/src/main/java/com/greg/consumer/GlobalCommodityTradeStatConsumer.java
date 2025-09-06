package com.greg.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

// todo: Service or Component?
@Service
public class GlobalCommodityTradeStatConsumer {

    // todo: don't use field injection, use constructor injection
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.agg-topic}")
    private String aggTopic;

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Nested Map: country -> {flowType -> totalTradeUSD}
    private final Map<String, Map<String, Double>> aggregates = new HashMap<>();

    // todo: track through the use of groupId
    // todo: also why is the signature different here?
    @KafkaListener(topics = "${app.kafka.topic}", groupId = "trade-consumer-group")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            String value = record.value();
            System.out.println("Received message: " + value);

            if (value.contains("\"type\":\"DONE\"")) {
                System.out.println("DONE message received, sending aggregates...");
                String json = new ObjectMapper().writeValueAsString(aggregates);
                kafkaTemplate.send(aggTopic, json);
                return;
            }

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

    // todo: I think this is just debug, check though
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


