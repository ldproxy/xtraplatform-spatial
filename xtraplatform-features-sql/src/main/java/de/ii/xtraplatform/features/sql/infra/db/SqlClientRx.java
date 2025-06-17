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
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.app.FeatureDataSql;
import de.ii.xtraplatform.features.sql.domain.SqlClient;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Transformer;
import io.reactivex.rxjava3.core.Flowable;
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
import java.util.function.BiFunction;
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

    // TODO encapsulating the query in a transaction is a workaround for what appears to be a bug in
    //      rxjava3-jdbc, see https://github.com/interactive-instruments/ldproxy/issues/1293
    Flowable<SqlRow> flowable =
        session
            .select(query)
            .get(
                resultSet -> {
                  SqlRow row = new SqlRowVals(collator).read(resultSet, options);

                  if (LOGGER.isDebugEnabled(MARKER.SQL_RESULT) && logBuffer.size() < 10) {
                    logBuffer.add(row);
                  }

                  return row;
                });

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

    int[] i = {0};
    BiFunction<ResultSet, String, String> mapper =
        (slickRow, previousId) -> {
          LOGGER.debug("QUERY {}", i[0]);
          // null not allowed as return value
          String id = null;
          try {
            id = slickRow.getString(1);
            LOGGER.debug("RETURNED {}", id);
            idConsumers.get(i[0]).accept(id);
            // LOGGER.debug("VALUES {}", values);
            LOGGER.debug("");
          } catch (SQLException e) {
            e.printStackTrace();
          }
          i[0]++;

          return previousId != null ? previousId : id;
        };

    String first = statements.get(0).get();
    if (LOGGER.isDebugEnabled(MARKER.SQL)) {
      LOGGER.debug(MARKER.SQL, "Executing statement: {}", first);
    }

    Flowable<? extends Tx<?>> txFlowable =
        // TODO: when implementing crud for joins, check if bug in rxjava3-jdbc still exists:
        //  when using returnGeneratedKeys, TransactedConnection.commit is called too often,
        //  therefore close is never called because counter < 0

        // for update/replace, the first statement is always delete, so we can skip the
        // returnGeneratedKeys
        /*statements.size() == 2
        ? session.update(first).transacted().tx().filter(tx -> !tx.isComplete())
        :*/ session
            .update(first)
            .transacted()
            .returnGeneratedKeys()
            .get(resultSet -> mapper.apply(resultSet, null))
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

                return tx.update(next)
                    .returnGeneratedKeys()
                    .get(
                        resultSet ->
                            mapper.apply(
                                resultSet,
                                tx.value() instanceof String ? (String) tx.value() : null))
                    .filter(tx2 -> !tx2.isComplete());
              });
    }

    Flowable<String> flowable = txFlowable.map(tx -> featureId.orElse((String) tx.value()));

    return Reactive.Source.publisher(flowable);
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

              List<Supplier<String>> statements =
                  m.stream()
                      .map(
                          queryFunction ->
                              Objects.isNull(queryFunction.get().first())
                                  ? null
                                  : (Supplier<String>) () -> queryFunction.get().first())
                      .filter(Objects::nonNull)
                      .collect(Collectors.toList());

              List<Consumer<String>> idConsumers =
                  m.stream()
                      .map(
                          queryFunction -> {
                            // TODO
                            Tuple<String, Consumer<String>> query = queryFunction.get();
                            return query.second();
                          })
                      .filter(Objects::nonNull)
                      .collect(Collectors.toList());

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
