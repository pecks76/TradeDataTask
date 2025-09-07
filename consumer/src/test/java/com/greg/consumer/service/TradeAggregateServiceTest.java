package com.greg.consumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TradeAggregateServiceTest {

    private TradeAggregateService tradeAggregateService;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        tradeAggregateService = new TradeAggregateService();
        objectMapper = new ObjectMapper();
    }

    @Test
    void testSingleRecordAggregation() throws Exception {
        String jsonStr = "{\"country_or_area\":\"Bulgaria\",\"flow\":\"Import\",\"trade_usd\":1000}";
        JsonNode jsonNode = objectMapper.readTree(jsonStr);

        tradeAggregateService.processRecord(jsonNode);

        Map<String, Map<String, Double>> aggregates = tradeAggregateService.getAggregates();
        assertEquals(1, aggregates.size());
        assertEquals(1000.0, aggregates.get("Bulgaria").get("Import"));
    }

    @Test
    void testMultipleRecordsAggregation() throws Exception {
        String json1 = "{\"country_or_area\":\"Bulgaria\",\"flow\":\"Import\",\"trade_usd\":1000}";
        String json2 = "{\"country_or_area\":\"Bulgaria\",\"flow\":\"Export\",\"trade_usd\":500}";
        String json3 = "{\"country_or_area\":\"Bulgaria\",\"flow\":\"Import\",\"trade_usd\":300}";
        String json4 = "{\"country_or_area\":\"France\",\"flow\":\"Import\",\"trade_usd\":200}";

        tradeAggregateService.processRecord(objectMapper.readTree(json1));
        tradeAggregateService.processRecord(objectMapper.readTree(json2));
        tradeAggregateService.processRecord(objectMapper.readTree(json3));
        tradeAggregateService.processRecord(objectMapper.readTree(json4));

        Map<String, Map<String, Double>> aggregates = tradeAggregateService.getAggregates();
        assertEquals(2, aggregates.size());
        assertEquals(1300.0, aggregates.get("Bulgaria").get("Import")); // 1000 + 300
        assertEquals(500.0, aggregates.get("Bulgaria").get("Export"));
        assertEquals(200.0, aggregates.get("France").get("Import"));
    }

    @Test
    void testProcessRecord_missingTradeUsd() throws Exception {
        JsonNode json = objectMapper.readTree("{\"country_or_area\":\"Bulgaria\",\"flow\":\"Export\"}");
        tradeAggregateService.processRecord(json);

        assertEquals(0, tradeAggregateService.getAggregates().get("Bulgaria").get("Export"));
    }

    @Test
    void testProcessRecord_invalidTradeUsd() throws Exception {
        JsonNode json = objectMapper.readTree("{\"country_or_area\":\"Bulgaria\",\"flow\":\"Export\",\"trade_usd\":\"abc\"}");
        tradeAggregateService.processRecord(json);

        assertEquals(0, tradeAggregateService.getAggregates().get("Bulgaria").get("Export"));
    }

    @Test
    void testProcessRecord_missingFields() throws Exception {
        JsonNode json = objectMapper.readTree("{}");
        tradeAggregateService.processRecord(json);

        assertTrue(tradeAggregateService.getAggregates().containsKey("UNKNOWN"));
        assertTrue(tradeAggregateService.getAggregates().get("UNKNOWN").containsKey("UNKNOWN"));
        assertEquals(0, tradeAggregateService.getAggregates().get("UNKNOWN").get("UNKNOWN"));
    }
}

