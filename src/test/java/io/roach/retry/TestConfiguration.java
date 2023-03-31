package io.roach.retry;

import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@EnableAutoConfiguration
@ComponentScan(basePackageClasses = TestConfiguration.class)
@Configuration
public class TestConfiguration {
    @Bean
    public TransactionRetryAspect transactionRetryAspect() {
        return new TransactionRetryAspect();
    }

    @Bean
    public OrderRepository orderRepository() {
        return Mockito.mock(OrderRepository.class);
    }
}
