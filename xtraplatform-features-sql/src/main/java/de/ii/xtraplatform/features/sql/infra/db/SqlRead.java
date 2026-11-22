/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The lifecycle of one streamed read: the connection is leased from the pool, the statement is
 * executed on it, and when the read ends, the statement is cancelled if it is still running and the
 * connection is returned. The database keeps working on a statement whose connection was merely
 * returned to the pool, so a read that ends before its rows were exhausted - the consumer or the
 * stall timeout cancelled it - has to cancel its statement first.
 */
final class SqlRead {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlRead.class);

  private final Connection connection;
  private final boolean transaction;
  private final AtomicReference<Statement> running;

  private SqlRead(Connection connection, boolean transaction) {
    this.connection = connection;
    this.transaction = transaction;
    this.running = new AtomicReference<>();
  }

  /**
   * Leases a connection. A read in a transaction lets the database driver use a server-side cursor
   * and stream the rows.
   */
  static SqlRead lease(DataSource dataSource, boolean transaction) throws SQLException {
    Connection connection = dataSource.getConnection();

    if (transaction) {
      try {
        connection.setAutoCommit(false);
      } catch (SQLException e) {
        close(connection);
        throw e;
      }
    }

    return new SqlRead(connection, transaction);
  }

  @SuppressWarnings({"PMD.CloseResource", "PMD.AvoidCatchingGenericException"})
  ResultSet execute(String query, int fetchSize) throws SQLException {
    Statement statement = connection.createStatement();

    try {
      if (fetchSize > 0) {
        statement.setFetchSize(fetchSize);
      }

      running.set(statement);

      return statement.executeQuery(query);
    } catch (SQLException | RuntimeException e) {
      running.set(null);
      close(statement);
      throw e;
    }
  }

  /** All rows were read, the statement is no longer running. */
  void exhausted() {
    running.set(null);
  }

  /**
   * Cancels the statement if it is still running and returns the connection to the pool. With a
   * server-side cursor the statement is idle between two fetches and the cancellation has no
   * effect.
   */
  void release() {
    cancel();

    if (transaction) {
      try {
        // nothing to commit, and a rollback is the cheapest way to close the server-side cursor
        connection.rollback();
      } catch (SQLException e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Ending the read transaction failed: {}", e.getMessage());
        }
      }
      try {
        connection.setAutoCommit(true);
      } catch (SQLException e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Resetting autocommit failed: {}", e.getMessage());
        }
      }
    }

    close(connection);
  }

  @SuppressWarnings("PMD.CloseResource")
  private void cancel() {
    Statement statement = running.getAndSet(null);

    if (Objects.isNull(statement)) {
      return;
    }

    try {
      statement.cancel();
    } catch (SQLException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Cancelling the statement failed: {}", e.getMessage());
      }
    }
  }

  /** Closes the result set and the statement it belongs to. */
  @SuppressWarnings("PMD.CloseResource")
  static void close(ResultSet resultSet) {
    if (Objects.isNull(resultSet)) {
      return;
    }

    Statement statement = null;

    try {
      statement = resultSet.getStatement();
    } catch (SQLException e) {
      // the result set is closed below regardless
    }

    close((AutoCloseable) resultSet);
    close(statement);
  }

  @SuppressWarnings("PMD.AvoidCatchingGenericException")
  private static void close(AutoCloseable closeable) {
    if (Objects.nonNull(closeable)) {
      try {
        closeable.close();
      } catch (Exception e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Closing {} failed: {}", closeable.getClass().getSimpleName(), e.getMessage());
        }
      }
    }
  }
}
