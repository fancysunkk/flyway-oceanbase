package com.github.fancysunkk.flyway.database.oceanbase;

import lombok.CustomLog;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.concurrent.Callable;

@CustomLog
public class MySQLNamedLockTemplate {
    /**
     * The connection for the named lock.
     */
    private final JdbcTemplate jdbcTemplate;

    private final String lockName;

    /**
     * Creates a new named lock template for this connection.
     *
     * @param jdbcTemplate  The jdbcTemplate for the connection.
     * @param discriminator A number to discriminate between locks.
     */
    MySQLNamedLockTemplate(JdbcTemplate jdbcTemplate, int discriminator) {
        this.jdbcTemplate = jdbcTemplate;
        lockName = "Flyway-" + discriminator;
    }

    /**
     * Executes this callback with a named lock.
     *
     * @param callable The callback to execute.
     * @return The result of the callable code.
     */
    public <T> T execute(Callable<T> callable) {
        try {
            lock();
            return callable.call();
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to acquire MySQL named lock: " + lockName, e);
        } catch (Exception e) {
            RuntimeException rethrow;
            if (e instanceof RuntimeException) {
                rethrow = (RuntimeException) e;
            } else {
                rethrow = new FlywayException(e);
            }
            throw rethrow;
        } finally {
            try {
                jdbcTemplate.execute("SELECT RELEASE_LOCK('" + lockName + "')");
            } catch (SQLException e) {
                LOG.error("Unable to release MySQL named lock: " + lockName, e);
            }
        }
    }

    private void lock() throws SQLException {
        while (!tryLock()) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                throw new FlywayException("Interrupted while attempting to acquire MySQL named lock: " + lockName, e);
            }
        }
    }

    private boolean tryLock() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT GET_LOCK(?,10)", lockName) == 1;
    }
}