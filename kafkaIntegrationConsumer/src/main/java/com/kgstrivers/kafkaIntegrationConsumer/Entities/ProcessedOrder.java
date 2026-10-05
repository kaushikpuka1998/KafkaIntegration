package com.kgstrivers.kafkaIntegrationConsumer.Entities;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

import java.time.LocalDateTime;

// One row per orderId already handled. The PK is the dedupe guard.
@Entity
@Table(name = "processed_orders")
@Data
public class ProcessedOrder {

    @Id
    private Long orderId;

    private LocalDateTime processedAt;
}
