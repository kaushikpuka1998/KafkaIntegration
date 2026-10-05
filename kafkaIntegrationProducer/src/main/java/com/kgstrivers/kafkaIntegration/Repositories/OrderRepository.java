package com.kgstrivers.kafkaIntegration.Repositories;

import com.kgstrivers.kafkaIntegration.Entities.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
}
