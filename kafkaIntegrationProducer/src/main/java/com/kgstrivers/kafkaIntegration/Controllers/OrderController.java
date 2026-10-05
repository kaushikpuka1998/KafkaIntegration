package com.kgstrivers.kafkaIntegration.Controllers;

import com.kgstrivers.kafkaIntegration.Entities.Order;
import com.kgstrivers.kafkaIntegration.Repositories.OrderRepository;
import com.kgstrivers.kafkaIntegration.Services.OrderService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public Order createOrder(@RequestBody Order order) {
        return orderService.createOrder(order);
    }

    @GetMapping
    public List<Order> getOrders() {
        return orderService.getAllOrders();
    }
}
