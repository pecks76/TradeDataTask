package com.greg.consumer.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.greg.consumer.service.TradeAggregateService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Map;

import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"trade-data-topic"})
class ConsumerIT {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TradeAggregateService tradeAggregateService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testConsumerAggregatesTradeData() throws Exception {

        String[] messages = new String[] {
                "{\"country_or_area\":\"Bulgaria\", \"flow\":\"Import\", \"trade_usd\":500}",
                "{\"country_or_area\":\"Bulgaria\", \"flow\":\"Import\", \"trade_usd\":500}",
                "{\"country_or_area\":\"Germany\", \"flow\":\"Export\", \"trade_usd\":300}",
                "{\"country_or_area\":\"Germany\", \"flow\":\"Export\", \"trade_usd\":200}"
        };

        for (String msg : messages) {
            kafkaTemplate.send("trade-data-topic", msg);
        }

        // Wait for the message to be consumed
        // Wait and assert safely
        await().atMost(10, SECONDS).untilAsserted(() -> {
            var aggregates = tradeAggregateService.getAggregates();

            // Safe nested map access
            double total = aggregates
                    .getOrDefault("Bulgaria", Map.of())
                    .getOrDefault("Import", 0.0);
            assertEquals(1000.0, total);

            double germanyExport = aggregates
                    .getOrDefault("Germany", Map.of())
                    .getOrDefault("Export", 0.0);
            assertEquals(500.0, germanyExport);
        });
    }
}
