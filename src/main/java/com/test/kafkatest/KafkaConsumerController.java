package com.test.kafkatest;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Slf4j
@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "test-topic",groupId = "consumer_group02" )
    public void consume(String message) throws IOException {
        log.info("Consumed Message :{}", message);
    }
}
