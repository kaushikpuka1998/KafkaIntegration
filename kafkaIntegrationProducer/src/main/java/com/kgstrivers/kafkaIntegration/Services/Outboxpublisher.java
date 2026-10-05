package com.kgstrivers.kafkaIntegration.Services;

import com.kgstrivers.kafkaIntegration.Entities.OutboxEvent;
import com.kgstrivers.kafkaIntegration.Events.OrderCreatedEvent;
import com.kgstrivers.kafkaIntegration.Kafka.OrderKafkaProducer;
import com.kgstrivers.kafkaIntegration.Repositories.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

@Slf4j
@Service
public class Outboxpublisher {
    private final OutboxEventRepository outboxEventRepository;
    private final OrderKafkaProducer orderKafkaProducer;
    private final ObjectMapper objectMapper;

    public Outboxpublisher(OutboxEventRepository outboxEventRepository, OrderKafkaProducer orderKafkaProducer, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.orderKafkaProducer = orderKafkaProducer;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedRate = 5000)
    public void publishOutboxEvents() {
        log.info("publishOutboxEvents");
        List<OutboxEvent> events =
                outboxEventRepository.findByStatus("PENDING");

        for (OutboxEvent outboxEvent : events) {

            try {

                OrderCreatedEvent event =
                        objectMapper.readValue(
                                outboxEvent.getPayload(),
                                OrderCreatedEvent.class
                        );

                orderKafkaProducer.publishOrderCreated(event);

                outboxEvent.setStatus("PUBLISHED");

                outboxEventRepository.save(outboxEvent);

            } catch (Exception e) {

                // Keep it PENDING.
                // It will be retried during the next execution.

                System.out.println(
                        "Failed to publish outbox event "
                                + outboxEvent.getId()
                );
            }
        }
    }
}
