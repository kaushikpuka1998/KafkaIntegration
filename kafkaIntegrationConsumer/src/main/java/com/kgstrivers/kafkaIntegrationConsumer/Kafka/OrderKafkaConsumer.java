package com.kgstrivers.kafkaIntegrationConsumer.Kafka;

import com.kgstrivers.kafkaIntegrationConsumer.Events.OrderCreatedEvent;
import com.kgstrivers.kafkaIntegrationConsumer.Repositories.ProcessedOrderRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OrderKafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderKafkaConsumer.class.getName());

    private final ProcessedOrderRepository processedOrderRepository;

    public OrderKafkaConsumer(ProcessedOrderRepository processedOrderRepository) {
        this.processedOrderRepository = processedOrderRepository;
    }

    @KafkaListener(
            topics = "order-created",
            groupId = "order-service"
    )
    @Transactional
    public void consume(
            ConsumerRecord<String, OrderCreatedEvent> record) {

        OrderCreatedEvent event = record.value();

        // Claim the orderId first; the marker and the business work commit together,
        // so a crash before commit rolls both back and the redelivery is processed again.
        if (processedOrderRepository.markProcessed(event.getOrderId()) == 0) {
            log.info("Duplicate orderId={} skipped (partition={}, offset={})",
                    event.getOrderId(), record.partition(), record.offset());
            return;
        }

        System.out.println(
                "Message = " + event
                        + ", Partition = " + record.partition()
                        + ", Offset = " + record.offset()
                        + ", Key = " + record.key()
        );
    }

}
