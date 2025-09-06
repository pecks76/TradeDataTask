package com.greg.consumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class TradeAggregateService {

    // Map<Country, Map<Flow, TotalUSD>>
    private final Map<String, Map<String, Double>> aggregates = new HashMap<>();

    public void processRecord(JsonNode json) {
        String country = json.get("country_or_area").asText();
        String flow = json.get("flow").asText();
        double tradeUsd = json.has("trade_usd") && !json.get("trade_usd").asText().isEmpty()
                ? json.get("trade_usd").asDouble()
                : 0.0;

        aggregates
                .computeIfAbsent(country, k -> new HashMap<>())
                .merge(flow, tradeUsd, Double::sum);
    }

    public Map<String, Map<String, Double>> getAggregates() {
        return aggregates;
    }
}
