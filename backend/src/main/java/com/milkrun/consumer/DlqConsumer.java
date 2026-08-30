package com.milkrun.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.milkrun.model.GpsEvent;
import com.milkrun.persistence.DeadLetterRepository;
import com.milkrun.pipeline.ReorderBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

/**
 * Processes late GPS events from the reorder buffer's DLQ stream.
 *
 * Late events are events that arrive after the reorder buffer's grace window
 * has closed. They are logged to the dead_letter_log table for audit purposes
 * and potential reconciliation.
 */
@Service
public class DlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumer.class);

    private final ReorderBuffer reorderBuffer;
    private final DeadLetterRepository deadLetterRepository;
    private final ObjectMapper objectMapper;

    public DlqConsumer(ReorderBuffer reorderBuffer, DeadLetterRepository deadLetterRepository,
                       ObjectMapper objectMapper) {
        this.reorderBuffer = reorderBuffer;
        this.deadLetterRepository = deadLetterRepository;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void startDlqProcessing() {
        log.info("Starting DLQ consumer for late events...");

        reorderBuffer.lateEvents()
                .flatMap(event -> {
                    String payload;
                    try {
                        payload = objectMapper.writeValueAsString(event);
                    } catch (Exception e) {
                        payload = "{}";
                    }
                    return deadLetterRepository.logDeadLetter(
                            "gps-events",
                            event.vanId(),
                            payload,
                            "LATE_ARRIVAL"
                    );
                })
                .doOnError(e -> log.error("DLQ consumer error: {}", e.getMessage()))
                .retry()
                .subscribe();

        log.info("DLQ consumer started");
    }
}
