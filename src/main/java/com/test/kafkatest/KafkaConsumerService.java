package com.test.kafkatest;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final SseEmitters sseEmitters;

    @KafkaListener(topics = "test-topic",groupId = "consumer_group02" )
    public void consume(String message) {
        log.info("리스너 {} ", message);
        sendMessageToClients(message);
    }


    public void addEmitter(SseEmitter emitter) {
        this.sseEmitters.add(emitter);
    }

    private void sendMessageToClients(String message) {
        for (SseEmitter emitter : sseEmitters.getEmitters()) {
            try {
                emitter.send(SseEmitter.event().name("message").data(message));
                log.info("send {}", message);
           } catch (IOException e) {
                log.error("send {} failed", message, e);
                emitter.completeWithError(e);
            }
        }
    }
}
