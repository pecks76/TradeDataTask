package com.greg.consumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Getter
@Service
public class TradeAggregateService {

    // Map<Country, Map<Flow, TotalUSD>>
    private final Map<String, Map<String, Double>> aggregates = new HashMap<>();

    public void processRecord(JsonNode json) {
        try {
            String country = json.hasNonNull("country_or_area") ? json.get("country_or_area").asText() : "UNKNOWN";
            String flow = json.hasNonNull("flow") ? json.get("flow").asText() : "UNKNOWN";

            double tradeUsd = 0.0;
            if (json.has("trade_usd") && !json.get("trade_usd").asText().isEmpty()) {
                try {
                    tradeUsd = json.get("trade_usd").asDouble();
                } catch (NumberFormatException e) {
                    log.warn("Invalid trade_usd value: {}", json.get("trade_usd").asText());
                }
            }

            aggregates
                    .computeIfAbsent(country, k -> new HashMap<>())
                    .merge(flow, tradeUsd, Double::sum);

        } catch (Exception e) {
            log.error("Failed to process record: {}", json.toString(), e);
        }
    }
}
