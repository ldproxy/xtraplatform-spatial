/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.domain.FeatureProviderConnector;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql.Dialect;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlRowMeta.Builder;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Source;
import de.ii.xtraplatform.streams.domain.Reactive.Transformer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

public interface SqlConnector
    extends FeatureProviderConnector<SqlRow, SqlQueryBatch, SqlQueryOptions> {

  int getMaxConnections();

  int getMinConnections();

  int getQueueSize();

  Dialect getDialect();

  SqlClient getSqlClient();

  @Override
  default Reactive.Source<SqlRow> getSourceStream(SqlQueryBatch queryBatch) {
    return getSourceStream(queryBatch, SqlQueryOptions.single());
  }

  // TODO: simplify, class SqlQueryRunner, remove options, singleFeature
  @Override
  default Reactive.Source<SqlRow> getSourceStream(
      SqlQueryBatch queryBatch, SqlQueryOptions options) {
    final long[] featureCount = {queryBatch.getLimit()};
    final long[] numberSkipped = {0L};
    final String[] lastTable = {""};
    final long[] lastNumberReturned = {0L};
    final long[] lastNumberSkipped = {0L};
    final boolean[] noOffset = {false};

    Source<SqlRow> sqlRowSource1 =
        Source.iterable(queryBatch.getQuerySets())
            .via(
                Transformer.flatMap(
                    querySet -> {
                      String currentTable = querySet.getTableSchemas().get(0).getName();

                      long found = lastNumberReturned[0] + lastNumberSkipped[0];

                      if (featureCount[0] <= 0
                          || (Objects.equals(lastTable[0], currentTable)
                              && found < queryBatch.getChunkSize())) {
                        return Source.empty();
                      }

                      long ns = numberSkipped[0];
                      // same table, nothing returned yet
                      if (Objects.equals(lastTable[0], currentTable)
                          && featureCount[0] == queryBatch.getLimit()) {
                        ns = 0L;
                      }
                      // next table, something returned before, and every following round
                      else if (noOffset[0]
                          || (!Objects.equals(lastTable[0], currentTable)
                              && featureCount[0] < queryBatch.getLimit())) {
                        ns = queryBatch.getOffset();
                        noOffset[0] = true;
                      }

                      return getMetaResult(
                              querySet.getMetaQuery().apply(featureCount[0], ns),
                              options,
                              currentTable)
                          .via(
                              Transformer.map(
                                  metaResult -> {
                                    featureCount[0] -= metaResult.getNumberReturned();
                                    numberSkipped[0] =
                                        numberSkipped[0] + metaResult.getNumberSkipped().orElse(0);
                                    lastTable[0] = currentTable;
                                    lastNumberReturned[0] = metaResult.getNumberReturned();
                                    lastNumberSkipped[0] = metaResult.getNumberSkipped().orElse(0);

                                    return Tuple.of(querySet, metaResult);
                                  }));
                    }))
            .via(
                Transformer.reduce(
                    Tuple.of(new ArrayList<SqlQuerySet>(), new ArrayList<SqlRowMeta>()),
                    (reducedTuple, nextTuple) -> {
                      List<SqlQuerySet> querySets = reducedTuple.first();
                      List<SqlRowMeta> rows = reducedTuple.second();
                      SqlQuerySet nextQuerySet = nextTuple.first();
                      SqlRowMeta nextRow = nextTuple.second();

                      if (rows.isEmpty()) {
                        rows.add(new Builder().numberReturned(0).build());
                      }

                      long numberReturned =
                          rows.get(0).getNumberReturned() + nextRow.getNumberReturned();
                      OptionalLong numberMatched =
                          rows.get(0).getNumberMatched().isEmpty()
                                  && nextRow.getNumberMatched().isEmpty()
                              ? OptionalLong.empty()
                              : !Objects.equals(rows.get(0).getName(), nextRow.getName())
                                  ? OptionalLong.of(
                                      rows.get(0).getNumberMatched().orElse(0)
                                          + nextRow.getNumberMatched().orElse(0))
                                  : rows.get(0).getNumberMatched();
                      OptionalLong numberSkipped3 =
                          rows.get(0).getNumberSkipped().isEmpty()
                                  && nextRow.getNumberSkipped().isEmpty()
                              ? OptionalLong.empty()
                              : !Objects.equals(rows.get(0).getName(), nextRow.getName())
                                  ? OptionalLong.of(
                                      rows.get(0).getNumberSkipped().orElse(0)
                                          + nextRow.getNumberSkipped().orElse(0))
                                  : rows.get(0).getNumberSkipped();

                      rows.set(
                          0,
                          new Builder()
                              .name(nextRow.getName())
                              .numberReturned(numberReturned)
                              .numberMatched(numberMatched)
                              .numberSkipped(numberSkipped3)
                              .build());

                      querySets.add(nextQuerySet);
                      rows.add(nextRow);

                      return reducedTuple;
                    }))
            .via(
                Transformer.flatMap(
                    plan -> {
                      if (queryBatch.isSingleFeature()) {
                        List<SqlQuerySet> querySets = queryBatch.getQuerySets();
                        ImmutableSqlRowMeta sqlRowMeta =
                            getMetaQueryResult(0L, 0L, 0L, 0L, -1L).build();
                        return Source.iterable(
                                IntStream.range(0, querySets.size())
                                    .boxed()
                                    .collect(Collectors.toList()))
                            .via(
                                Transformer.flatMap(
                                    index -> {
                                      int[] i = {0};
                                      Source<SqlRow>[] sqlRows =
                                          querySets
                                              .get(index)
                                              .getValueQueries()
                                              .apply(sqlRowMeta, 0L, 0L)
                                              .map(
                                                  valueQuery ->
                                                      getSqlClient()
                                                          .getSourceStream(
                                                              valueQuery,
                                                              new ImmutableSqlQueryOptions.Builder()
                                                                  .from(options)
                                                                  .tableSchema(
                                                                      querySets
                                                                          .get(index)
                                                                          .getTableSchemas()
                                                                          .get(i[0]))
                                                                  .type(
                                                                      querySets
                                                                          .get(index)
                                                                          .getOptions()
                                                                          .getType())
                                                                  .containerPriority(i[0]++)
                                                                  .build()))
                                              .toArray(
                                                  (IntFunction<Source<SqlRow>[]>) Source[]::new);

                                      return mergeAndSort(sqlRows);
                                    }))
                            .prepend(Source.single(sqlRowMeta));
                      }

                      List<SqlQuerySet> querySets = plan.first();
                      SqlRowMeta aggregatedMetaResult = plan.second().get(0);
                      List<SqlRowMeta> metaResults = plan.second().subList(1, plan.second().size());
                      final long[] featureCount2 = {queryBatch.getLimit()};
                      final long[] numberSkipped2 = {0L};
                      final String[] lastTable2 = {""};

                      if (options.isHitsOnly()) {
                        return Source.single(aggregatedMetaResult);
                      }

                      return Source.iterable(
                              IntStream.range(0, querySets.size())
                                  .boxed()
                                  .collect(Collectors.toList()))
                          .via(
                              Transformer.flatMap(
                                  index -> {
                                    String currentTable =
                                        querySets.get(index).getTableSchemas().get(0).getName();
                                    featureCount2[0] -= metaResults.get(index).getNumberReturned();
                                    numberSkipped2[0] =
                                        Objects.equals(lastTable2[0], currentTable)
                                            ? numberSkipped2[0]
                                            : numberSkipped2[0]
                                                + metaResults
                                                    .get(index)
                                                    .getNumberSkipped()
                                                    .orElse(0);
                                    // TODO
                                    long ns =
                                        Objects.equals(lastTable2[0], currentTable)
                                            ? numberSkipped2[0]
                                            : featureCount2[0] < queryBatch.getLimit()
                                                ? queryBatch.getOffset()
                                                : numberSkipped2[0];
                                    lastTable2[0] = currentTable;

                                    if (metaResults.get(index).getNumberReturned() <= 0) {
                                      return Source.empty();
                                    }

                                    int[] i = {0};
                                    Source<SqlRow>[] sqlRows =
                                        querySets
                                            .get(index)
                                            .getValueQueries()
                                            .apply(metaResults.get(index), featureCount2[0], ns)
                                            .map(
                                                valueQuery ->
                                                    getSqlClient()
                                                        .getSourceStream(
                                                            valueQuery,
                                                            new ImmutableSqlQueryOptions.Builder()
                                                                .from(options)
                                                                .tableSchema(
                                                                    querySets
                                                                        .get(index)
                                                                        .getTableSchemas()
                                                                        .get(i[0]))
                                                                .type(
                                                                    querySets
                                                                        .get(index)
                                                                        .getOptions()
                                                                        .getType())
                                                                .containerPriority(i[0]++)
                                                                .build()))
                                            .toArray((IntFunction<Source<SqlRow>[]>) Source[]::new);

                                    return mergeAndSort(sqlRows);
                                  }))
                          .prepend(Source.single(aggregatedMetaResult));
                    }));

    return sqlRowSource1.mapError(PSQL_CONTEXT);
  }

  // TODO: simplify
  default Reactive.Source<SqlRowMeta> getMetaResult(
      Optional<String> metaQuery, SqlQueryOptions options, String table) {
    if (!metaQuery.isPresent()) {
      return Reactive.Source.single(getMetaQueryResult(0L, 0L, 0L, 0L, -1L).build());
    }

    List<Class<?>> columnTypes =
        Stream.concat(
                IntStream.range(0, 2 + (options.getCustomSortKeys().size() * 2))
                    .mapToObj(i -> Object.class),
                Stream.of(Long.class, Long.class, Long.class))
            .collect(Collectors.toList());

    return getSqlClient()
        .getSourceStream(metaQuery.get(), SqlQueryOptions.withColumnTypes(columnTypes))
        .via(Reactive.Transformer.map(sqlRow -> getMetaQueryResult(sqlRow.getValues(), table)));
  }

  default Builder getMetaQueryResult(
      Object minKey, Object maxKey, Long numberReturned, Long numberMatched, Long numberSkipped) {
    return new ImmutableSqlRowMeta.Builder()
        .minKey(minKey)
        .maxKey(maxKey)
        .numberReturned(Objects.nonNull(numberReturned) ? numberReturned : 0L)
        .numberMatched(
            Objects.nonNull(numberMatched) && numberMatched > -1
                ? OptionalLong.of(numberMatched)
                : OptionalLong.empty())
        .numberSkipped(
            Objects.nonNull(numberSkipped) && numberSkipped > -1
                ? OptionalLong.of(numberSkipped)
                : OptionalLong.empty());
  }

  default SqlRowMeta getMetaQueryResult(List<Object> values, String table) {
    int size = values.size();
    Builder builder =
        getMetaQueryResult(
            values.get(size - 5),
            values.get(size - 4),
            (Long) values.get(size - 3),
            (Long) values.get(size - 2),
            (Long) values.get(size - 1));

    for (int i = 0; i < size - 5; i = i + 2) {
      builder.addCustomMinKeys(values.get(i)).addCustomMaxKeys(values.get(i + 1));
    }

    return builder.name(table).build();
  }

  static <T extends Comparable<T>> Reactive.Source<T> mergeAndSort(Reactive.Source<T>... sources) {
    if (sources.length == 1) {
      return sources[0];
    }

    return mergeAndSort(sources[0], sources[1], Arrays.asList(sources).subList(2, sources.length));
  }

  static <T extends Comparable<T>> Reactive.Source<T> mergeAndSort(
      Reactive.Source<T> source1, Reactive.Source<T> source2, Iterable<Reactive.Source<T>> rest) {
    Comparator<T> comparator = Comparator.naturalOrder();
    Reactive.Source<T> mergedAndSorted = source1.mergeSorted(source2, comparator);
    for (Reactive.Source<T> source3 : rest) {
      mergedAndSorted = mergedAndSorted.mergeSorted(source3, comparator);
    }
    return mergedAndSorted;
  }

  Function<Throwable, Throwable> PSQL_CONTEXT =
      throwable -> {
        if (throwable instanceof PSQLException) {
          PSQLException e = (PSQLException) throwable;
          String message =
              Optional.ofNullable(e.getServerErrorMessage())
                  .map(
                      serverErrorMessage -> {
                        StringBuilder totalMessage = new StringBuilder("\n  ");
                        String msg = serverErrorMessage.getSeverity();
                        if (msg != null) {
                          totalMessage.append(msg).append(": ");
                        }
                        msg = serverErrorMessage.getMessage();
                        if (msg != null) {
                          totalMessage.append(msg);
                        }
                        msg = serverErrorMessage.getDetail();
                        if (msg != null) {
                          totalMessage.append("\n  ").append("Detail: ").append(msg);
                        }

                        msg = serverErrorMessage.getHint();
                        if (msg != null) {
                          totalMessage.append("\n  ").append("Hint: ").append(msg);
                        }
                        msg = String.valueOf(serverErrorMessage.getPosition());
                        if (!msg.equals("0")) {
                          totalMessage.append("\n  ").append("Position: ").append(msg);
                        }
                        msg = serverErrorMessage.getWhere();
                        if (msg != null) {
                          totalMessage.append("\n  ").append("Where: ").append(msg);
                        }
                        return totalMessage.toString();
                      })
                  .orElseGet(e::getMessage);

          return new PSQLException(
              "Unexpected SQL query error: " + message, PSQLState.UNKNOWN_STATE, throwable);
        }
        return throwable;
      };
}
