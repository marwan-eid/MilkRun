package com.milkrun.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import com.milkrun.model.DispatchRequest;
import com.milkrun.model.DispatchEvent;

@RestController
@RequestMapping("/api/dispatch")
@CrossOrigin(origins = "*")
public class DispatchController {

    private static final Logger log = LoggerFactory.getLogger(DispatchController.class);
    private static final String TOPIC = "dispatch-events";
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public DispatchController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public Mono<Void> dispatchOrder(@RequestBody DispatchRequest request) {
        try {
            DispatchEvent event = new DispatchEvent(request.latitude(), request.longitude());
            String payload = objectMapper.writeValueAsString(event);
            log.info("Ad-Hoc route explicitly intercepted: {}", payload);
            kafkaTemplate.send(TOPIC, event.eventId(), payload);
        } catch (Exception e) {
            log.error("Failed to serialize and broadcast dispatch payload natively", e);
        }
        return Mono.empty();
    }
}
