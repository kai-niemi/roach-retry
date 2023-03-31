package io.roach.retry;

import org.springframework.core.annotation.AliasFor;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.lang.annotation.*;

/**
 * Indicates the annotated class or method is a transactional service boundary. It's architectural role is to
 * delegate to control services or repositories to perform actual business logic processing in
 * the context of a new transaction.
 * <p/>
 * Marks the annotated class as {@link org.springframework.transaction.annotation.Transactional @Transactional}
 * with propagation level {@link org.springframework.transaction.annotation.Propagation#REQUIRES_NEW REQUIRES_NEW},
 * clearly indicating that a new transaction is started before method entry.
 * <p>
 * The @Transactional annotation can be overridden at method level.
 */
@Inherited
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Transactional(propagation = Propagation.REQUIRES_NEW)
public @interface TransactionBoundary {
    @AliasFor(annotation = Transactional.class, attribute = "propagation")
    Propagation propagation() default Propagation.REQUIRES_NEW;

    /**
     * @return number of times to retry aborted transient data access exceptions with exponential backoff.
     */
    int retryAttempts() default 10;

    /**
     * @return max backoff time in millis per retry cycle
     */
    long maxBackoffMillis() default 30_000;
}
