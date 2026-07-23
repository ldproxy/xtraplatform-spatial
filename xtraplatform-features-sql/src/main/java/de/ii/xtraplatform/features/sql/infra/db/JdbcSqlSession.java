/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import de.ii.xtraplatform.base.domain.LogContext.MARKER;
import de.ii.xtraplatform.features.domain.FeatureMutationHookException;
import de.ii.xtraplatform.features.sql.domain.SqlSession;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcSqlSession implements SqlSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSqlSession.class);

  // Safety cap on accumulated batch size — pathological feature shouldn't grow unbounded.
  private static final int MAX_BATCH_SIZE = 1000;

  private final Connection connection;
  private boolean finalised;
  private Savepoint activeSavepoint;
  // Non-fatal SQL warnings (e.g. PostgreSQL RAISE WARNING / RAISE NOTICE) emitted by mutation
  // statements, accumulated until the caller drains them.
  private final List<String> pendingWarnings = new ArrayList<>();

  JdbcSqlSession(Connection connection) {
    this.connection = connection;
    this.finalised = false;
    try {
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      try {
        connection.close();
      } catch (SQLException ignore) {
        // suppressed
      }
      throw new IllegalStateException("Could not start SQL session", e);
    }
  }

  @Override
  public String run(
      List<Supplier<String>> statements,
      List<Consumer<String>> idConsumers,
      Optional<String> featureId) {
    if (finalised) {
      throw new IllegalStateException("SQL session is closed");
    }
    String firstGeneratedId = null;
    Statement batchStmt = null;
    List<String> batchedSql = new ArrayList<>();
    List<Consumer<String>> batchedConsumers = new ArrayList<>();

    try {
      for (int i = 0; i < statements.size(); i++) {
        String sql = statements.get(i).get();
        if (Objects.isNull(sql)) {
          continue;
        }
        Consumer<String> consumer = i < idConsumers.size() ? idConsumers.get(i) : null;

        if (isBatchable(sql)) {
          try {
            if (batchStmt == null) {
              batchStmt = connection.createStatement();
            }
            batchStmt.addBatch(sql);
          } catch (SQLException e) {
            throw new IllegalStateException(
                "Mutation statement failed: " + e.getMessage() + " — statement: " + sql, e);
          }
          batchedSql.add(sql);
          batchedConsumers.add(consumer);
          if (batchedSql.size() >= MAX_BATCH_SIZE) {
            flushBatch(batchStmt, batchedSql, batchedConsumers);
          }
          continue;
        }

        // Non-batchable: flush any pending batch first so ordering is preserved.
        if (!batchedSql.isEmpty()) {
          flushBatch(batchStmt, batchedSql, batchedConsumers);
        }

        if (LOGGER.isDebugEnabled(MARKER.SQL)) {
          LOGGER.debug(MARKER.SQL, "Executing statement: {}", sql);
        }
        try (Statement statement = connection.createStatement()) {
          boolean hasResultSet = statement.execute(sql);
          harvestWarnings(statement);
          String returnedId = null;
          if (hasResultSet) {
            try (ResultSet rs = statement.getResultSet()) {
              if (rs.next()) {
                returnedId = rs.getString(1);
              }
            }
          }
          if (consumer != null) {
            consumer.accept(returnedId);
          }
          if (firstGeneratedId == null && returnedId != null) {
            firstGeneratedId = returnedId;
          }
        } catch (SQLException e) {
          throw new IllegalStateException(
              "Mutation statement failed: " + e.getMessage() + " — statement: " + sql, e);
        }
      }

      if (!batchedSql.isEmpty()) {
        flushBatch(batchStmt, batchedSql, batchedConsumers);
      }
    } finally {
      if (batchStmt != null) {
        try {
          batchStmt.close();
        } catch (SQLException ignore) {
          // suppressed
        }
      }
    }
    return featureId.orElse(firstGeneratedId);
  }

  @Override
  public List<String> runReturning(String sql) {
    if (finalised) {
      throw new IllegalStateException("SQL session is closed");
    }
    if (LOGGER.isDebugEnabled(MARKER.SQL)) {
      LOGGER.debug(MARKER.SQL, "Executing statement: {}", sql);
    }
    try (Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute(sql);
      harvestWarnings(statement);
      if (!hasResultSet) {
        return List.of();
      }
      List<String> ids = new ArrayList<>();
      try (ResultSet rs = statement.getResultSet()) {
        while (rs.next()) {
          ids.add(rs.getString(1));
        }
      }
      return ids;
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Mutation statement failed: " + e.getMessage() + " — statement: " + sql, e);
    }
  }

  @Override
  public List<String> execute(List<String> statements) {
    if (finalised) {
      throw new IllegalStateException("SQL session is closed");
    }
    List<String> warnings = new ArrayList<>();
    for (String sql : statements) {
      if (LOGGER.isDebugEnabled(MARKER.SQL)) {
        LOGGER.debug(MARKER.SQL, "Executing hook statement: {}", sql);
      }
      try (Statement statement = connection.createStatement()) {
        statement.execute(sql);
        for (SQLWarning w = statement.getWarnings(); w != null; w = w.getNextWarning()) {
          warnings.add(w.getMessage());
        }
      } catch (SQLException e) {
        // Expected, configuration-driven failure (e.g. a check function RAISE EXCEPTION) — carry
        // the warnings collected so far so they survive the failure path.
        throw new FeatureMutationHookException(
            "Hook statement failed: " + e.getMessage() + " — statement: " + sql, e, warnings);
      }
    }
    return warnings;
  }

  // Generators emit "RETURNING null" for child / junction / FK-update statements — these have
  // no caller-meaningful return value and their consumers are no-ops. Main inserts use
  // "RETURNING <pk>" and must run individually so their generated id can drive child SQL.
  private static boolean isBatchable(String sql) {
    String trimmed = sql.trim();
    if (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }
    return trimmed.toLowerCase(Locale.ROOT).endsWith("returning null");
  }

  /**
   * Collects the non-fatal SQL warning chain of a just-executed statement into {@link
   * #pendingWarnings} and clears it — the batch statement is reused across flushes, so its chain
   * would otherwise be harvested again on the next flush.
   */
  private void harvestWarnings(Statement statement) {
    try {
      for (SQLWarning w = statement.getWarnings(); w != null; w = w.getNextWarning()) {
        pendingWarnings.add(w.getMessage());
      }
      statement.clearWarnings();
    } catch (SQLException e) {
      LOGGER.debug("Reading SQL warnings failed: {}", e.getMessage());
    }
  }

  @Override
  public List<String> drainWarnings() {
    if (pendingWarnings.isEmpty()) {
      return List.of();
    }
    List<String> drained = List.copyOf(pendingWarnings);
    pendingWarnings.clear();
    return drained;
  }

  private void flushBatch(
      Statement batchStmt, List<String> batchedSql, List<Consumer<String>> batchedConsumers) {
    if (batchedSql.isEmpty()) {
      return;
    }
    if (LOGGER.isDebugEnabled(MARKER.SQL)) {
      LOGGER.debug(
          MARKER.SQL, "Executing batched statements ({}): {}", batchedSql.size(), batchedSql);
    }
    try {
      batchStmt.executeBatch();
      batchStmt.clearBatch();
      harvestWarnings(batchStmt);
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Batched mutation failed: " + e.getMessage() + " — first statement: " + batchedSql.get(0),
          e);
    }
    // Preserve the per-statement consumer contract: each batched statement returns no id, so
    // call consumers with null in order (in practice these are no-ops for child/junction/FK
    // generators, but the contract allows any Consumer<String>).
    for (Consumer<String> c : batchedConsumers) {
      if (c != null) {
        c.accept(null);
      }
    }
    batchedSql.clear();
    batchedConsumers.clear();
  }

  @Override
  public void savepoint() {
    if (finalised) {
      throw new IllegalStateException("SQL session is closed");
    }
    if (activeSavepoint != null) {
      throw new IllegalStateException("A savepoint is already active on this SQL session");
    }
    try {
      activeSavepoint = connection.setSavepoint();
    } catch (SQLException e) {
      throw new IllegalStateException("Savepoint failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void releaseSavepoint() {
    if (finalised) {
      throw new IllegalStateException("SQL session is closed");
    }
    if (activeSavepoint == null) {
      throw new IllegalStateException("No savepoint is active on this SQL session");
    }
    try {
      connection.releaseSavepoint(activeSavepoint);
    } catch (SQLException e) {
      throw new IllegalStateException("Savepoint release failed: " + e.getMessage(), e);
    } finally {
      activeSavepoint = null;
    }
  }

  @Override
  public void rollbackToSavepoint() {
    if (finalised) {
      throw new IllegalStateException("SQL session is closed");
    }
    if (activeSavepoint == null) {
      throw new IllegalStateException("No savepoint is active on this SQL session");
    }
    try {
      connection.rollback(activeSavepoint);
      connection.releaseSavepoint(activeSavepoint);
    } catch (SQLException e) {
      throw new IllegalStateException("Savepoint rollback failed: " + e.getMessage(), e);
    } finally {
      activeSavepoint = null;
    }
  }

  @Override
  public void commit() {
    if (finalised) {
      throw new IllegalStateException("SQL session is closed");
    }
    try {
      connection.commit();
      finalised = true;
    } catch (SQLException e) {
      throw new IllegalStateException("Commit failed: " + e.getMessage(), e);
    } finally {
      releaseConnection();
    }
  }

  @Override
  public void rollback() {
    if (finalised) {
      return;
    }
    try {
      connection.rollback();
    } catch (SQLException e) {
      LOGGER.warn("Rollback failed: {}", e.getMessage());
    } finally {
      finalised = true;
      releaseConnection();
    }
  }

  @Override
  public void close() {
    if (!finalised) {
      rollback();
    } else {
      releaseConnection();
    }
  }

  private void releaseConnection() {
    try {
      if (!connection.isClosed()) {
        connection.setAutoCommit(true);
        connection.close();
      }
    } catch (SQLException e) {
      LOGGER.debug("Connection close failed: {}", e.getMessage());
    }
  }
}
