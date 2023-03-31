package io.roach.retry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.PessimisticLockingFailureException;

import java.sql.SQLException;

import static org.mockito.Mockito.times;

@SpringBootTest(classes = TestConfiguration.class)
@Configuration
public class TransactionRetryAspectTest {
    @Autowired
    private OrderService businessService;

    @Autowired
    private OrderRepository orderRepository;

    @BeforeEach
    public void resetMocks() {
        Mockito.reset(orderRepository);
    }

    @Test
    public void whenCallingTransactionBoundary_expectRetriesAndThenSuccess() {
        Mockito.doThrow(new PessimisticLockingFailureException("Error!").initCause(
                        new SQLException("Conflict!", "40001")))
                .doThrow(new PessimisticLockingFailureException("Error!!").initCause(
                        new SQLException("Conflict!!", "40001")))
                .doThrow(new PessimisticLockingFailureException("Error!!!").initCause(
                        new SQLException("Conflict!!!", "40001")))
                .doNothing()
                .when(orderRepository)
                .updateStatus(Mockito.anyLong(), Mockito.anyString());

        businessService.updateOrderStatus(1L, "NEW");
        Mockito.verify(orderRepository, times(4))
                .updateStatus(Mockito.anyLong(), Mockito.anyString());
    }

    @Test
    public void whenCallingTransactionBoundaryWithTooManyErrors_expectGivingUpException() {
        Mockito.doThrow(new PessimisticLockingFailureException("Error!").initCause(
                        new SQLException("Conflict!", "40001")))
                .doThrow(new PessimisticLockingFailureException("Error!!").initCause(
                        new SQLException("Conflict!!", "40001")))
                .doThrow(new PessimisticLockingFailureException("Error!!!").initCause(
                        new SQLException("Conflict!!!", "40001")))
                .doThrow(new PessimisticLockingFailureException("Error!!!").initCause(
                        new SQLException("Conflict!!!", "40001")))
                .doNothing()
                .when(orderRepository)
                .updateStatus(Mockito.anyLong(), Mockito.anyString());

        Assertions.assertThrowsExactly(ConcurrencyFailureException.class, () -> {
            businessService.updateOrderStatus(1L, "NEW");
        });

        Mockito.verify(orderRepository, times(4))
                .updateStatus(Mockito.anyLong(), Mockito.anyString());
    }
}
