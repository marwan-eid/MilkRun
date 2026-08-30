package com.milkrun.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import com.milkrun.model.DispatchRequest;
import com.milkrun.model.DispatchEvent;

@RestController
@RequestMapping("/api/dispatch")
public class DispatchController {

    private static final Logger log = LoggerFactory.getLogger(DispatchController.class);
    private static final String TOPIC = "dispatch-events";
    private final ReactiveKafkaProducerTemplate<String, Object> kafkaTemplate;

    public DispatchController(ReactiveKafkaProducerTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public Mono<Void> dispatchOrder(@RequestBody DispatchRequest request) {
        DispatchEvent event = new DispatchEvent(request.latitude(), request.longitude());
        log.info("Ad-Hoc route explicitly intercepted: {}", event);
        return kafkaTemplate.send(TOPIC, event.eventId(), event).then();
    }
}
