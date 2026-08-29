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
import java.sql.SQLException;
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
import org.davidmoten.rxjava3.jdbc.Database;
import org.davidmoten.rxjava3.jdbc.internal.DelegatedConnection;
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

  private final Database session;
  private final SqlDbmsAdapter dbmsAdapter;
  private final SqlDialect dialect;
  private final Collator collator;

  public SqlClientRx(
      Database session,
      SqlDbmsAdapter dbmsAdapter,
      SqlDialect dialect,
      Optional<String> defaultCollation) {
    this.session = session;
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
      session
          .update(query)
          .complete()
          .subscribe(() -> result.complete(ImmutableList.of()), result::completeExceptionally);

      return result;
    }

    session
        .select(query)
        .get(resultSet -> new SqlRowVals(collator).read(resultSet, options))
        .toList()
        .subscribe(result::complete, result::completeExceptionally);

    return result;
  }

  @Override
  public Reactive.Source<SqlRow> getSourceStream(String query, SqlQueryOptions options) {
    if (LOGGER.isDebugEnabled(MARKER.SQL)) {
      LOGGER.debug(MARKER.SQL, "Executing statement: {}", query);
    }
    List<SqlRow> logBuffer = new ArrayList<>(5);

    org.davidmoten.rxjava3.jdbc.ResultSetMapper<SqlRow> mapper =
        resultSet -> {
          SqlRow row = new SqlRowVals(collator).read(resultSet, options);

          if (LOGGER.isDebugEnabled(MARKER.SQL_RESULT) && logBuffer.size() < 10) {
            logBuffer.add(row);
          }

          return row;
        };

    // A positive fetch size requires a transaction so the database driver uses a server-side cursor
    // and streams rows instead of buffering the whole result set in memory (PostgreSQL ignores the
    // fetch size with autoCommit=true).
    // TODO encapsulating the query in a transaction is also a workaround for what appears to be a
    //      bug in rxjava3-jdbc, see https://github.com/interactive-instruments/ldproxy/issues/1293
    Flowable<SqlRow> flowable =
        options.getFetchSize() > 0
            ? session
                .select(query)
                .transacted()
                .fetchSize(options.getFetchSize())
                .valuesOnly()
                .get(mapper)
            : session.select(query).get(mapper);

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

    // The blocking connection provider runs connect+execute+read on the subscribing thread, so
    // without this the whole stream is single-threaded. Subscribing on a worker thread lets several
    // parallel-flagged queries (e.g. the concurrent single-shot value phase) run at once, each on
    // its
    // own connection.
    if (options.isParallel()) {
      flowable = flowable.subscribeOn(Schedulers.io());
    }

    // Safety net for a read that neither completes nor fails. A database error raised while the
    // rows are being streamed is not delivered by the underlying library (see the issue linked
    // above), so the stream can stall forever: no error is logged, no response is sent, and the
    // connections the sub-query holds stay held until the client gives up. A connection lost
    // mid-stream — a failover in a replicated cluster, for instance — looks exactly the same.
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

  @Override
  public Connection getConnection() {
    return session.connection().blockingGet();
  }

  @Override
  public SqlSession openSession() {
    Connection connection = session.connection().blockingGet();
    return new JdbcSqlSession(connection);
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

    if (actualConnection instanceof DelegatedConnection) {
      actualConnection = ((DelegatedConnection) actualConnection).con();
    }
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
