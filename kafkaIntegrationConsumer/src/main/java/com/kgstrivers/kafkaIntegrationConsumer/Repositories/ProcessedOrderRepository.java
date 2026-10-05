package com.kgstrivers.kafkaIntegrationConsumer.Repositories;

import com.kgstrivers.kafkaIntegrationConsumer.Entities.ProcessedOrder;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

public interface ProcessedOrderRepository extends JpaRepository<ProcessedOrder, Long> {

    // Atomic claim: returns 1 if this orderId is new, 0 if it was already processed.
    @Modifying
    @Query(value = "INSERT INTO processed_orders (order_id, processed_at) VALUES (:orderId, now()) ON CONFLICT DO NOTHING",
            nativeQuery = true)
    int markProcessed(Long orderId);
}
