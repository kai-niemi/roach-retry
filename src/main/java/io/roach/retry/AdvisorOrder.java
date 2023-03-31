package io.roach.retry;

import org.springframework.core.Ordered;

/**
 * Ordering constants for transaction advisors.
 */
public interface AdvisorOrder {
    int TRANSACTION_RETRY_ADVISOR = Ordered.LOWEST_PRECEDENCE - 4;

    int TRANSACTION_ADVISOR = Ordered.LOWEST_PRECEDENCE - 3;
}
