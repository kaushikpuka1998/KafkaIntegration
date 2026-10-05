package com.kgstrivers.kafkaIntegrationConsumer.Kafka;

import com.kgstrivers.kafkaIntegrationConsumer.Events.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderKafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderKafkaConsumer.class.getName());


    @KafkaListener(
            topics = "order-created",
            groupId = "order-service"
    )


    public void consume(
            ConsumerRecord<String, OrderCreatedEvent> record) {

        System.out.println(
                "Message = " + record.value()
                        + ", Partition = " + record.partition()
                        + ", Offset = " + record.offset()
                        + ", Key = " + record.key()
        );
    }

}
