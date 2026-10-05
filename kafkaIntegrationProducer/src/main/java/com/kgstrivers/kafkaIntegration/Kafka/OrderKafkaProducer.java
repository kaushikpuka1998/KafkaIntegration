package com.kgstrivers.kafkaIntegration.Kafka;

import com.kgstrivers.kafkaIntegration.Events.OrderCreatedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderKafkaProducer {

    private static final String TOPIC = "order-created";
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;


    public OrderKafkaProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    public void publishOrderCreated(OrderCreatedEvent orderCreatedEvent) {
        kafkaTemplate
                .send("order-created", orderCreatedEvent.getOrderId().toString(), orderCreatedEvent)
                .whenComplete((result, exception) -> {

                    if (exception == null) {

                        System.out.println("========== SUCCESS ==========");
                        System.out.println("Kafka ACK received");
                        System.out.println(
                                "Partition: " +
                                        result.getRecordMetadata().partition()
                        );
                        System.out.println(
                                "Offset: " +
                                        result.getRecordMetadata().offset()
                        );

                    } else {

                        System.out.println("========== FAILURE ==========");
                        System.out.println(
                                "Kafka send failed: " +
                                        exception.getClass().getName()
                        );
                        System.out.println(
                                "Message: " +
                                        exception.getMessage()
                        );
                    }
                });
    }
}
