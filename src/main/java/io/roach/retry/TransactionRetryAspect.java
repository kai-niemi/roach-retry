package io.roach.retry;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.Order;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

/**
 * AOP aspect that automatically retries operations that throw transient SQL exceptions
 * with state code 40001. It applies an around-advice for all {@link TransactionBoundary}
 * annotated methods and intercepts and retries concurrency failures such as
 * deadlock looser, pessimistic and optimistic locking failures.
 * <p>
 * Concurrency related failures are more common for databases running in higher isolation
 * levels such as 1SR when a workload is contended.
 * <p>
 * The main pre-condition is that no existing transaction can be in scope when attempting
 * a retry, hence it's applicable for transaction boundaries only. The business operation
 * retried must also be idempotent since it can be invoked more than once due to a retry.
 * <p>
 * This advice must be applied before the Spring transaction advisor in the call chain.
 * See {@link org.springframework.transaction.annotation.EnableTransactionManagement} for
 * controlling weaving order.
 */
@Aspect
@Order(TransactionRetryAspect.PRECEDENCE)
public class TransactionRetryAspect {
    /**
     * The precedence at which this advice is ordered by which also controls
     * the order it is invoked in the call chain between a source and target.
     */
    public static final int PRECEDENCE = AdvisorOrder.TRANSACTION_RETRY_ADVISOR;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Pointcut expression matching all transactional boundaries.
     */
    @Pointcut("execution(public * *(..)) "
            + "&& @annotation(transactionBoundary)")
    public void anyTransactionBoundaryOperation(TransactionBoundary transactionBoundary) {
    }

    @Around(value = "anyTransactionBoundaryOperation(transactionBoundary)", argNames = "pjp,transactionBoundary")
    public Object doRetryableOperation(ProceedingJoinPoint pjp, TransactionBoundary transactionBoundary) throws Throwable {
        Assert.isTrue(!TransactionSynchronizationManager.isActualTransactionActive(),
                "Detected active transaction for [" + pjp.getSignature().toShortString()
                        + "] - check retry advice @Order and @EnableTransactionManagement order. The retry advice " +
                        "must be in front of the transactional advice (higher priority) in the call chain.");

        // Grab from type if needed (for non-annotated methods)
        if (transactionBoundary == null) {
            transactionBoundary = AnnotationUtils.findAnnotation(pjp.getSignature().getDeclaringType(),
                    TransactionBoundary.class);
        }

        Assert.notNull(transactionBoundary, "No @TransactionBoundary annotation found!?");

        // Verify tx propagation, check if there's a method-level override
        MethodSignature signature = (MethodSignature) pjp.getSignature();
        Transactional transactional = AnnotationUtils.findAnnotation(signature.getMethod(), Transactional.class);
        if (transactional != null) {
            Propagation propagation = transactional.propagation();
            Assert.isTrue(propagation.equals(Propagation.REQUIRES_NEW),
                    "Expected Propagation.REQUIRES_NEW for @Transactional method "
                            + pjp.getSignature().toShortString());
        } else {
            Propagation propagation = transactionBoundary.propagation();
            Assert.isTrue(propagation.equals(Propagation.REQUIRES_NEW),
                    "Expected Propagation.REQUIRES_NEW for @TransactionBoundary method "
                            + pjp.getSignature().toShortString());
        }

        int numRetries = 0;
        final String methodName = pjp.getSignature().toShortString();
        final Instant callTime = Instant.now();

        do {
            final Throwable throwable;
            try {
                Object rv = pjp.proceed(); // Invoke target and start new transaction

                if (numRetries > 0) {
                    handleRecovery(numRetries, methodName, Duration.between(callTime, Instant.now()));
                }

                return rv;
            } catch (UndeclaredThrowableException ex) {
                throwable = ex.getUndeclaredThrowable();
            } catch (Exception ex) { // Catch r/w and commit time exceptions
                throwable = ex;
            }

            Throwable cause = NestedExceptionUtils.getMostSpecificCause(throwable);
            if (cause instanceof SQLException) {
                SQLException sqlException = (SQLException) cause;
                if (isRetryable(sqlException)) {
                    numRetries++;
                    handleTransientException(sqlException, numRetries, methodName, transactionBoundary);
                } else {
                    handleNonTransientException(sqlException, methodName);
                    throw throwable;
                }
            } else {
                throw throwable;
            }
        } while (numRetries <= transactionBoundary.retryAttempts());

        throw new ConcurrencyFailureException(
                "Too many serialization errors (" + numRetries + " of max " + transactionBoundary.retryAttempts()
                        + ") for method '" + pjp.getSignature().toShortString()
                        + "'. Giving up!");
    }

    private boolean isRetryable(SQLException sqlException) {
        // PSQLState.SERIALIZATION_FAILURE (40001) is the only state code we are looking for in terms of safe retries
        return "40001".equals(sqlException.getSQLState());
    }

    private void handleRecovery(int numCalls, String methodName, Duration elapsedTime) {
        logger.info("Recovered from transient SQL error after {} calls to method '{}' (time spent: {})",
                numCalls, methodName, elapsedTime);
    }

    private void handleTransientException(SQLException sqlException, int numRetries, String methodName,
                                          TransactionBoundary transactionBoundary) {
        try {
            long backoffMillis = Math.min((long) (Math.pow(2, numRetries) + Math.random() * 1000),
                    transactionBoundary.maxBackoffMillis());
            if (logger.isWarnEnabled()) {
                logger.warn("Transient SQL error ({}) in method '{}' (backoff for {} ms before retry attempt {}/{}): {}",
                        sqlException.getSQLState(),
                        methodName,
                        backoffMillis,
                        numRetries,
                        transactionBoundary.retryAttempts(),
                        sqlException.getMessage());
            }
            Thread.sleep(backoffMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handleNonTransientException(SQLException sqlException, String methodName) {
        sqlException.forEach(ex -> {
            SQLException nested = (SQLException) ex;
            logger.warn("Non-transient SQL error (state: {}) (code: {}) in method '{}': {}",
                    nested.getSQLState(),
                    nested.getErrorCode(),
                    methodName,
                    nested.getMessage());
        });
    }
}

