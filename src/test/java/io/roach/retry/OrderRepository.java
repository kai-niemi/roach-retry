package io.roach.retry;

import org.springframework.dao.DataAccessException;

public interface OrderRepository {
    void updateStatus(Long id, String status) throws DataAccessException;
}
