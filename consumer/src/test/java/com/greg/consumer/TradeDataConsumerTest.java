package com.greg.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.greg.consumer.service.TradeAggregateService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;

class TradeDataConsumerTest {

    private TradeAggregateService aggregatorService;
    private KafkaTemplate<String, String> kafkaTemplate;
    private TradeDataConsumer consumer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        aggregatorService = Mockito.mock(TradeAggregateService.class);
        kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        consumer = new TradeDataConsumer(aggregatorService, kafkaTemplate, "agg-topic");
        objectMapper = new ObjectMapper();
    }

    @Test
    void testConsumeRegularMessage() throws Exception {
        String message = "{\"country_or_area\":\"Bulgaria\",\"flow\":\"Import\",\"trade_usd\":1000}";
        consumer.consume(message);
        Mockito.verify(aggregatorService).processRecord(objectMapper.readTree(message));
        Mockito.verifyNoInteractions(kafkaTemplate); // Should not send aggregates yet
    }

    @Test
    void testConsumeDoneMessage() throws Exception {
        String doneMessage = "{\"type\":\"DONE\"}";
        consumer.consume(doneMessage);
        Mockito.verify(kafkaTemplate).send(Mockito.eq("agg-topic"), Mockito.anyString());
    }
}
