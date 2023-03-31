package io.roach.retry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OrderService {
    @Autowired
    private OrderRepository orderRepository;

    @TransactionBoundary(retryAttempts = 3)
    public Long updateOrderStatus(Long orderId, String status) {
        orderRepository.updateStatus(orderId, status);
        return orderId;
    }
}
