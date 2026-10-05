package com.kgstrivers.kafkaIntegration.Services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kgstrivers.kafkaIntegration.Entities.Order;
import com.kgstrivers.kafkaIntegration.Entities.OutboxEvent;
import com.kgstrivers.kafkaIntegration.Events.OrderCreatedEvent;
import com.kgstrivers.kafkaIntegration.Repositories.OrderRepository;
import com.kgstrivers.kafkaIntegration.Repositories.OutboxEventRepository;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class OrderService {
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(
            OrderRepository orderRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper) {

        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order createOrder(Order order) {
        Order savedOrder = orderRepository.save(order);
        OrderCreatedEvent event =  new OrderCreatedEvent(savedOrder.getId(), savedOrder.getProduct(),savedOrder.getAmount());
        OutboxEvent outboxEvent = new OutboxEvent();

        outboxEvent.setEventType("OrderCreated");
        outboxEvent.setAggregateId(savedOrder.getId());
        outboxEvent.setPayload(convertToJson(event));
        outboxEvent.setStatus("PENDING");
        outboxEvent.setCreatedAt(LocalDateTime.now());

        outboxEventRepository.save(outboxEvent);
        return order;
    }

    public List<Order> getAllOrders(){
        return orderRepository.findAll();
    }

    private String convertToJson(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize OrderCreatedEvent", e);
        }
    }
}
