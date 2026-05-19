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

  /** Commits all mutations performed against this session. */
  void commit();

  /** Rolls back all mutations performed against this session. Idempotent. */
  void rollback();

  /** Equivalent to {@link #rollback()} if {@link #commit()} has not been called. Idempotent. */
  @Override
  void close();
}
