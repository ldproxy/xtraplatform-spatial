/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import de.ii.xtraplatform.base.domain.LogContext;
import io.reactivex.rxjava3.core.Flowable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reference accounting for a single rxjava3-jdbc transacted mutation chain.
 *
 * <p>The connection behind such a chain is reference counted: the count starts at 1, every {@code
 * tx.update(...)} forks it (+1), and every statement stream that terminates commits or rolls back
 * (-1). The real COMMIT/ROLLBACK and the real close (which returns the connection to the pool)
 * happen only when the count reaches 0.
 *
 * <p>A statement stream that is cancelled instead of terminated fires neither handler, so its
 * reference is never released. That is exactly what happens to all preceding statements when a
 * later statement of the chain fails: the count never reaches 0, the transaction is neither
 * committed nor rolled back, and the pooled connection stays leased forever.
 *
 * <p>This guard mirrors the count from the outside: {@link #acquired()} for every reference taken,
 * {@link #track(Flowable)} to release it again on any terminal event, and {@link
 * #releaseIfLeaked()} at the end of the chain to drain whatever is left.
 */
class MutationTransactionGuard {

  private static final Logger LOGGER = LoggerFactory.getLogger(MutationTransactionGuard.class);

  private final AtomicInteger outstanding = new AtomicInteger(0);
  private final AtomicReference<Connection> connection = new AtomicReference<>();

  /** A reference was taken, either the initial connection or a fork from {@code tx.update(...)}. */
  void acquired() {
    outstanding.incrementAndGet();
  }

  /** A reference is released on any terminal event, but not on cancellation. */
  <T> Flowable<T> track(Flowable<T> stage) {
    return stage
        .doOnComplete(outstanding::decrementAndGet)
        .doOnError(throwable -> outstanding.decrementAndGet());
  }

  /** The transacted connection is only reachable through a running statement's result set. */
  void capture(ResultSet resultSet) throws SQLException {
    if (Objects.isNull(connection.get())) {
      Statement statement = resultSet.getStatement();

      if (Objects.nonNull(statement)) {
        connection.compareAndSet(null, statement.getConnection());
      }
    }
  }

  /** Drains references left behind by cancelled statement streams, no-op on the happy path. */
  void releaseIfLeaked() {
    int leaked = outstanding.getAndSet(0);

    if (leaked <= 0) {
      return;
    }

    Connection con = connection.get();

    if (Objects.isNull(con)) {
      LOGGER.debug(
          "Abandoned mutation transaction with {} unreleased reference(s), no connection captured",
          leaked);
      return;
    }

    LOGGER.warn(
        "Abandoned mutation transaction, rolling back and releasing the connection ({} unreleased reference(s))",
        leaked);

    try {
      for (int i = 0; i < leaked && !con.isClosed(); i++) {
        // performs the real ROLLBACK once the reference count reaches 0
        con.rollback();
        // no-op until the reference count is 0, then returns the connection to the pool
        con.close();
      }

      if (!con.isClosed()) {
        LOGGER.error(
            "Could not release the connection of an abandoned mutation transaction, the connection pool may become depleted");
      }
    } catch (SQLException e) {
      LogContext.errorAsWarn(LOGGER, e, "Error releasing an abandoned mutation transaction");
    }
  }
}
