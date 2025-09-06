package com.greg.consumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TradeAggregatorServiceTest {

    private TradeAggregateService aggregatorService;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        aggregatorService = new TradeAggregateService();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testSingleRecordAggregation() throws Exception {
        String jsonStr = "{\"country_or_area\":\"Bulgaria\",\"flow\":\"Import\",\"trade_usd\":1000}";
        JsonNode jsonNode = objectMapper.readTree(jsonStr);

        aggregatorService.processRecord(jsonNode);

        Map<String, Map<String, Double>> aggregates = aggregatorService.getAggregates();
        assertEquals(1, aggregates.size());
        assertEquals(1000.0, aggregates.get("Bulgaria").get("Import"));
    }

    @Test
    void testMultipleRecordsAggregation() throws Exception {
        String json1 = "{\"country_or_area\":\"Bulgaria\",\"flow\":\"Import\",\"trade_usd\":1000}";
        String json2 = "{\"country_or_area\":\"Bulgaria\",\"flow\":\"Export\",\"trade_usd\":500}";
        String json3 = "{\"country_or_area\":\"Bulgaria\",\"flow\":\"Import\",\"trade_usd\":300}";
        String json4 = "{\"country_or_area\":\"France\",\"flow\":\"Import\",\"trade_usd\":200}";

        aggregatorService.processRecord(objectMapper.readTree(json1));
        aggregatorService.processRecord(objectMapper.readTree(json2));
        aggregatorService.processRecord(objectMapper.readTree(json3));
        aggregatorService.processRecord(objectMapper.readTree(json4));

        Map<String, Map<String, Double>> aggregates = aggregatorService.getAggregates();
        assertEquals(2, aggregates.size());
        assertEquals(1300.0, aggregates.get("Bulgaria").get("Import")); // 1000 + 300
        assertEquals(500.0, aggregates.get("Bulgaria").get("Export"));
        assertEquals(200.0, aggregates.get("France").get("Import"));
    }
}

