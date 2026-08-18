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
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.base.domain.LogContext.MARKER;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.app.FeatureDataSql;
import de.ii.xtraplatform.features.sql.domain.SqlClient;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import de.ii.xtraplatform.features.sql.domain.SqlSession;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Transformer;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.sql.Connection;
import java.sql.ResultSet;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.davidmoten.rxjava3.jdbc.Database;
import org.davidmoten.rxjava3.jdbc.Tx;
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
  public Reactive.Source<String> getMutationSource(
      List<Supplier<String>> statements,
      List<Consumer<String>> idConsumers,
      Object executionContext,
      Optional<String> featureId) {
    /*List<Function<FeatureSql, String>> toStatementsWithLog =
    statements.stream()
        .map(
            function ->
                (Function<FeatureSql, String>)
                    featureSql -> {
                      String statement = function.apply(featureSql);

                      if (LOGGER.isDebugEnabled(MARKER.SQL)) {
                        LOGGER.debug(MARKER.SQL, "Executing statement: {}", statement);
                      }

                      return statement;
                    })
        .collect(Collectors.toList());*/

    // rxjava3-jdbc does not release the transacted connection when a statement stream is cancelled,
    // which is what happens to all preceding statements when a later one fails, see the guard
    MutationTransactionGuard guard = new MutationTransactionGuard();

    String first = statements.get(0).get();
    if (LOGGER.isDebugEnabled(MARKER.SQL)) {
      LOGGER.debug(MARKER.SQL, "Executing statement: {}", first);
    }

    Flowable<? extends Tx<?>> txFlowable =
        guard
            .track(
                session
                    .update(first)
                    .transacted()
                    .returnGeneratedKeys()
                    .get(
                        resultSet -> {
                          guard.capture(resultSet);
                          return consumeId(resultSet, null, idConsumers, 0);
                        })
                    // the transacted connection is created when the statement stream is subscribed
                    .doOnSubscribe(subscription -> guard.acquired()))
            .filter(tx -> !tx.isComplete());

    for (int j = 1; j < statements.size(); j++) {
      int finalJ = j;
      txFlowable =
          txFlowable.flatMap(
              tx -> {
                String next = statements.get(finalJ).get();
                if (LOGGER.isDebugEnabled(MARKER.SQL)) {
                  LOGGER.debug(MARKER.SQL, "Executing statement: {}", next);
                }

                // tx.update forks the transacted connection
                guard.acquired();

                return guard
                    .track(
                        tx.update(next)
                            .returnGeneratedKeys()
                            .get(
                                resultSet -> {
                                  guard.capture(resultSet);
                                  return consumeId(
                                      resultSet,
                                      tx.value() instanceof String ? (String) tx.value() : null,
                                      idConsumers,
                                      finalJ);
                                }))
                    .filter(tx2 -> !tx2.isComplete());
              });
    }

    Flowable<String> flowable =
        txFlowable
            .map(tx -> featureId.orElse((String) tx.value()))
            .doFinally(guard::releaseIfLeaked);

    return Reactive.Source.publisher(flowable);
  }

  private static String consumeId(
      ResultSet resultSet, String previousId, List<Consumer<String>> idConsumers, int index) {
    // null not allowed as return value
    String id = null;

    try {
      id = resultSet.getString(1);

      if (index < idConsumers.size()) {
        Consumer<String> idConsumer = idConsumers.get(index);

        if (Objects.nonNull(idConsumer)) {
          idConsumer.accept(id);
        }
      } else if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("No id consumer for mutation statement {}, returned id: {}", index, id);
      }
    } catch (SQLException e) {
      LogContext.errorAsDebug(
          LOGGER, e, "Could not read the id returned by mutation statement {}", index);
    }

    return previousId != null ? previousId : id;
  }

  @Override
  public Transformer<FeatureDataSql, String> getMutationFlow(
      Function<FeatureDataSql, List<Supplier<Tuple<String, Consumer<String>>>>> mutations,
      Object executionContext,
      String primaryKey,
      Optional<String> id) {

    Reactive.Transformer<FeatureDataSql, String> toQueries =
        Reactive.Transformer.flatMap(
            feature -> {
              List<Supplier<Tuple<String, Consumer<String>>>> m = mutations.apply(feature);

              // both lists have to stay index aligned, the statements are resolved lazily since
              // they may depend on ids returned by preceding statements
              List<Supplier<String>> statements = new ArrayList<>();
              List<Consumer<String>> idConsumers = new ArrayList<>();

              for (Supplier<Tuple<String, Consumer<String>>> queryFunction : m) {
                Tuple<String, Consumer<String>> query = queryFunction.get();

                if (Objects.isNull(query.first())) {
                  continue;
                }

                statements.add(() -> queryFunction.get().first());
                idConsumers.add(query.second());
              }

              Optional<String> featureId =
                  feature
                      .getMapping()
                      .getColumnForId()
                      .flatMap(
                          idCol -> {
                            if (!Objects.equals(primaryKey, idCol.second().getName())
                                && feature
                                    .getRows()
                                    .get(0)
                                    .first()
                                    .getFullPath()
                                    .equals(idCol.first().getFullPath())) {
                              return Optional.ofNullable(
                                  feature
                                      .getRows()
                                      .get(0)
                                      .second()
                                      .getValues()
                                      .get(idCol.second().getName()));
                            }
                            return Optional.empty();
                          })
                      .map(SqlClientRx::unquote);

              return getMutationSource(statements, idConsumers, executionContext, featureId);
            });

    if (id.isPresent()) {
      // TODO: check that feature id equals given id
      Reactive.Transformer<FeatureDataSql, FeatureDataSql> filter =
          Reactive.Transformer.filter(featureSql -> true);

      return filter.via(toQueries);
    }

    return toQueries;
  }

  private static String unquote(String value) {
    if (value.startsWith("'") && value.endsWith("'")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
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
