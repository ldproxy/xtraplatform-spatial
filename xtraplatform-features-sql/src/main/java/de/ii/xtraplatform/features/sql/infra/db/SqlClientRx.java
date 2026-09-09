/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import com.google.common.collect.ImmutableList;
import com.zaxxer.hikari.pool.ProxyConnection;
import de.ii.xtraplatform.base.domain.LogContext.MARKER;
import de.ii.xtraplatform.features.sql.domain.SqlClient;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import de.ii.xtraplatform.features.sql.domain.SqlSession;
import de.ii.xtraplatform.streams.domain.Reactive;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlClientRx implements SqlClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlClientRx.class);

  // Generous on purpose: this is not a query budget but a stall detector, so it has to sit
  // well above the slowest legitimate gap between two rows (a count over a large table, a
  // sub-query whose first row needs a full scan). Only an unbounded wait is the failure mode
  // it exists to end.
  private static final long READ_STALL_TIMEOUT_MINUTES = 10;

  private final DataSource dataSource;
  private final SqlDbmsAdapter dbmsAdapter;
  private final SqlDialect dialect;
  private final Collator collator;

  public SqlClientRx(
      DataSource dataSource,
      SqlDbmsAdapter dbmsAdapter,
      SqlDialect dialect,
      Optional<String> defaultCollation) {
    this.dataSource = dataSource;
    this.dbmsAdapter = dbmsAdapter;
    this.dialect = dialect;
    this.collator = dbmsAdapter.getRowSortingCollator(defaultCollation);
  }

  @Override
  public CompletableFuture<Collection<SqlRow>> run(String query, SqlQueryOptions options) {
    if (LOGGER.isDebugEnabled(MARKER.SQL)) {
      LOGGER.debug(MARKER.SQL, "Executing statement: {}", query);
    }
    CompletableFuture<Collection<SqlRow>> result = new CompletableFuture<>();

    if (options.getColumnTypes().isEmpty()) {
      // a statement without a result (DDL, INSERT, DROP); autocommit is on, so it is committed when
      // it returns and the connection goes back to the pool in every case
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute(query);
        result.complete(ImmutableList.of());
      } catch (SQLException e) {
        result.completeExceptionally(e);
      }

      return result;
    }

    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      List<SqlRow> rows = new ArrayList<>();

      while (resultSet.next()) {
        rows.add(new SqlRowVals(collator).read(resultSet, options));
      }

      result.complete(rows);
    } catch (SQLException | RuntimeException e) {
      result.completeExceptionally(e);
    }

    return result;
  }

  @Override
  public Reactive.Source<SqlRow> getSourceStream(String query, SqlQueryOptions options) {
    if (LOGGER.isDebugEnabled(MARKER.SQL)) {
      LOGGER.debug(MARKER.SQL, "Executing statement: {}", query);
    }
    List<SqlRow> logBuffer = new ArrayList<>(5);

    // A positive fetch size requires a transaction so the database driver uses a server-side cursor
    // and streams rows instead of buffering the whole result set in memory (PostgreSQL ignores the
    // fetch size with autoCommit=true).
    boolean streamed = options.getFetchSize() > 0;

    // The connection is leased when the stream is subscribed and returned to the pool when it
    // terminates, whether the rows were exhausted, the read failed or the consumer cancelled.
    Flowable<SqlRow> flowable =
        Flowable.using(
            () -> lease(streamed),
            connection ->
                Flowable.generate(
                    () -> execute(connection, query, options.getFetchSize()),
                    (resultSet, emitter) -> {
                      if (resultSet.next()) {
                        SqlRow row = new SqlRowVals(collator).read(resultSet, options);

                        if (LOGGER.isDebugEnabled(MARKER.SQL_RESULT) && logBuffer.size() < 10) {
                          logBuffer.add(row);
                        }

                        emitter.onNext(row);
                      } else {
                        emitter.onComplete();
                      }
                    },
                    SqlClientRx::close),
            connection -> release(connection, streamed),
            true);

    // TODO: prettify, see
    // https://github.com/slick/slick/blob/main/slick/src/main/scala/slick/jdbc/StatementInvoker.scala
    if (LOGGER.isDebugEnabled(MARKER.SQL_RESULT)) {
      flowable =
          flowable.doOnComplete(
              () -> {
                LOGGER.debug(MARKER.SQL, "Executed statement: {}", query);
                for (int i = 0; i < logBuffer.size(); i++) {
                  if (i == 0) {
                    String columns =
                        Stream.concat(
                                logBuffer.get(i).getSortKeyNames().stream(),
                                logBuffer.get(i).getColumnPaths().stream()
                                    .map(path -> path.get(path.size() - 1)))
                            .collect(Collectors.joining(" | "));
                    LOGGER.debug(MARKER.SQL_RESULT, columns);
                  }
                  String values =
                      Stream.concat(
                              logBuffer.get(i).getSortKeys().stream()
                                  .map(val -> Objects.nonNull(val) ? val.toString() : "null"),
                              logBuffer.get(i).getValues().stream()
                                  .map(
                                      val ->
                                          Objects.nonNull(val)
                                              ? val.toString().length() > 100
                                                  ? (val.toString().substring(0, 100) + "...")
                                                  : val.toString()
                                              : "null"))
                          .collect(Collectors.joining(" | "));
                  LOGGER.debug(MARKER.SQL_RESULT, values);
                }
              });
    }

    // Lease, execute and read run on the subscribing thread, so without this the whole stream is
    // single-threaded. Subscribing on a worker thread lets several parallel-flagged queries (e.g.
    // the concurrent single-shot value phase) run at once, each on its own connection.
    if (options.isParallel()) {
      flowable = flowable.subscribeOn(Schedulers.io());
    }

    // Safety net for a read that neither completes nor fails: a connection lost mid-stream — a
    // failover in a replicated cluster, for instance — can leave the driver waiting for the next
    // row forever, so no error is logged, no response is sent, and the connections the sub-query
    // holds stay held until the client gives up.
    // The timeout is per element, not per stream, so a slow but progressing read is unaffected
    // however long it runs in total; only a gap longer than the window ends the stream, with an
    // error that does propagate. A database-side statement_timeout is no substitute: its error
    // would be swallowed the same way.
    flowable =
        flowable.timeout(
            READ_STALL_TIMEOUT_MINUTES,
            TimeUnit.MINUTES,
            Schedulers.computation(),
            Flowable.error(
                () ->
                    new IllegalStateException(
                        String.format(
                            "The database delivered no row for %d minutes and the read was ended; "
                                + "the statement is in the SQL debug log.",
                            READ_STALL_TIMEOUT_MINUTES))));

    return Reactive.Source.publisher(flowable);
  }

  private Connection lease(boolean transaction) throws SQLException {
    Connection connection = dataSource.getConnection();

    if (transaction) {
      try {
        connection.setAutoCommit(false);
      } catch (SQLException e) {
        close(connection);
        throw e;
      }
    }

    return connection;
  }

  private static ResultSet execute(Connection connection, String query, int fetchSize)
      throws SQLException {
    Statement statement = connection.createStatement();

    try {
      if (fetchSize > 0) {
        statement.setFetchSize(fetchSize);
      }

      return statement.executeQuery(query);
    } catch (SQLException | RuntimeException e) {
      close(statement);
      throw e;
    }
  }

  /** Ends a read-only transaction, if any, and returns the connection to the pool. */
  private static void release(Connection connection, boolean transaction) {
    if (transaction) {
      try {
        // nothing to commit, and a rollback is the cheapest way to close the server-side cursor
        connection.rollback();
      } catch (SQLException e) {
        LOGGER.debug("Ending the read transaction failed: {}", e.getMessage());
      }
      try {
        connection.setAutoCommit(true);
      } catch (SQLException e) {
        LOGGER.debug("Resetting autocommit failed: {}", e.getMessage());
      }
    }

    close(connection);
  }

  /** Closes the result set and the statement it belongs to. */
  private static void close(ResultSet resultSet) {
    Statement statement = null;

    try {
      statement = resultSet.getStatement();
    } catch (SQLException e) {
      // the result set is closed below regardless
    }

    close((AutoCloseable) resultSet);
    close(statement);
  }

  private static void close(AutoCloseable closeable) {
    if (Objects.nonNull(closeable)) {
      try {
        closeable.close();
      } catch (Exception e) {
        LOGGER.debug("Closing {} failed: {}", closeable.getClass().getSimpleName(), e.getMessage());
      }
    }
  }

  @Override
  public Connection getConnection() {
    return leaseConnection();
  }

  @Override
  public SqlSession openSession() {
    return new JdbcSqlSession(leaseConnection());
  }

  /** A pooled connection; closing it returns it to the pool. */
  private Connection leaseConnection() {
    try {
      return dataSource.getConnection();
    } catch (SQLException e) {
      throw new IllegalStateException(
          "Could not obtain a database connection: " + e.getMessage(), e);
    }
  }

  @Override
  public SqlDialect getSqlDialect() {
    return dialect;
  }

  @Override
  public SqlDbmsAdapter getDbmsAdapter() {
    return dbmsAdapter;
  }

  @Override
  public List<String> getNotifications(Connection connection) {
    Connection actualConnection = connection;

    if (actualConnection instanceof ProxyConnection) {
      try {
        actualConnection = actualConnection.unwrap(Connection.class);
      } catch (SQLException e) {
        // ignore
      }
    }

    if (actualConnection instanceof PGConnection) {
      try {
        return Arrays.stream(((PGConnection) actualConnection).getNotifications())
            .map(PGNotification::getParameter)
            .collect(Collectors.toList());
      } catch (SQLException e) {
        // ignore
      }
    }
    return ImmutableList.of();
  }
}
