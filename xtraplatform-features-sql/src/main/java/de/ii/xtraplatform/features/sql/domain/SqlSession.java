/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Synchronous, single-connection SQL session that executes mutation statements against one
 * underlying JDBC transaction. {@link #commit()} makes the changes durable; {@link #close()}
 * without a prior {@code commit()} rolls back.
 */
public interface SqlSession extends AutoCloseable {

  /**
   * Executes the given statements in order on this session's connection. Each statement is expected
   * to return a single string column (typically a {@code RETURNING} clause); the value is passed to
   * the corresponding entry in {@code idConsumers}.
   *
   * @param statements lazily evaluated SQL statements
   * @param idConsumers consumers receiving the returned id, one per statement
   * @param featureId optional caller-supplied id; if absent, the first generated id is returned
   * @return the feature id (either {@code featureId} or the first generated id)
   */
  String run(
      List<Supplier<String>> statements,
      List<Consumer<String>> idConsumers,
      Optional<String> featureId);

  /**
   * Executes a single SQL statement and returns every value in the first column of the result set,
   * preserving the database's row order. Intended for multi-row {@code INSERT ... RETURNING}
   * statements where the caller needs every generated id, not just the first.
   *
   * @param sql the statement to execute
   * @return one entry per result-set row (the first column, stringified); empty if the statement
   *     produces no result set
   */
  List<String> runReturning(String sql);

  /**
   * Executes raw statements in order on this session's connection, ignoring any result sets, and
   * returns the non-fatal SQL warnings they produced (e.g. PostgreSQL {@code RAISE WARNING} /
   * {@code RAISE NOTICE}). A statement that fails throws and leaves the transaction open for
   * rollback. Intended for transaction-lifecycle hooks (session setup, pre-commit checks).
   *
   * @param statements the statements to execute, in order
   * @return the collected warning messages across all statements, in execution order
   */
  List<String> execute(List<String> statements);

  /**
   * Returns the non-fatal SQL warnings (e.g. PostgreSQL {@code RAISE WARNING} / {@code RAISE
   * NOTICE}) that mutation statements run via {@link #run} / {@link #runReturning} have produced
   * since the last call, and clears them. Warnings produced by {@link #execute} are returned by
   * that method directly and do not show up here. The default implementation returns an empty list
   * for sessions that do not collect warnings.
   */
  default List<String> drainWarnings() {
    return List.of();
  }

  /**
   * Marks a recoverable point in this session's open transaction ({@code SAVEPOINT}). At most one
   * savepoint may be active at a time; it is consumed by either {@link #releaseSavepoint()} or
   * {@link #rollbackToSavepoint()}. The default implementation throws {@link
   * UnsupportedOperationException} for sessions that have not adopted the API.
   */
  default void savepoint() {
    throw new UnsupportedOperationException("Savepoints are not supported by this SQL session");
  }

  /**
   * Releases the active savepoint ({@code RELEASE SAVEPOINT}), keeping all changes made since
   * {@link #savepoint()} as part of the enclosing transaction. Throws when no savepoint is active.
   */
  default void releaseSavepoint() {
    throw new UnsupportedOperationException("Savepoints are not supported by this SQL session");
  }

  /**
   * Undoes all changes made since {@link #savepoint()} ({@code ROLLBACK TO SAVEPOINT}) and releases
   * the savepoint, leaving the enclosing transaction usable for further statements. Throws when no
   * savepoint is active.
   */
  default void rollbackToSavepoint() {
    throw new UnsupportedOperationException("Savepoints are not supported by this SQL session");
  }

  /** Commits all mutations performed against this session. */
  void commit();

  /** Rolls back all mutations performed against this session. Idempotent. */
  void rollback();

  /** Equivalent to {@link #rollback()} if {@link #commit()} has not been called. Idempotent. */
  @Override
  void close();
}
