/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.base.domain.resiliency.Volatile2;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.domain.FeatureProviderConnector;
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
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

public interface SqlConnector
    extends FeatureProviderConnector<SqlRow, SqlQueryBatch, SqlQueryOptions>, Volatile2 {

  int getMaxConnections();

  int getMinConnections();

  int getQueueSize();

  String getDialect();

  SqlClient getSqlClient();

  @Override
  default Reactive.Source<SqlRow> getSourceStream(SqlQueryBatch queryBatch) {
    return getSourceStream(queryBatch, SqlQueryOptions.single());
  }

  class Paging {
    private final long limit;
    private final long offset;
    private final long chunkSize;
    private final boolean allowSkipMetaQueries;
    private long featureCountdown;
    private long numberSkipped;
    private String lastTable;
    private long lastNumberReturned;
    private long lastNumberSkipped;
    private boolean noOffset;

    public Paging(long limit, long offset, long chunkSize, boolean allowSkipMetaQueries) {
      this.limit = limit;
      this.offset = offset;
      this.chunkSize = chunkSize;
      this.allowSkipMetaQueries = allowSkipMetaQueries;

      this.featureCountdown = limit;
      this.numberSkipped = 0L;
      this.lastTable = "";
      this.lastNumberReturned = 0L;
      this.lastNumberSkipped = 0L;
      this.noOffset = false;
    }

    Optional<Tuple<Long, Long>> get(String currentTable) {
      long found = lastNumberReturned + lastNumberSkipped;

      if (featureCountdown <= 0 || (Objects.equals(lastTable, currentTable) && found < chunkSize)) {
        return allowSkipMetaQueries ? Optional.empty() : Optional.of(Tuple.of(0L, offset));
      }

      long ns = numberSkipped;
      // same table, nothing returned yet
      if (Objects.equals(lastTable, currentTable) && featureCountdown == limit) {
        ns = 0L;
      }
      // next table, something returned before, and every following round
      else if (noOffset || (!Objects.equals(lastTable, currentTable) && featureCountdown < limit)) {
        ns = offset;
        noOffset = true;
      }

      return Optional.of(Tuple.of(featureCountdown, ns));
    }

    void register(String currentTable, SqlRowMeta metaResult) {
      featureCountdown -= metaResult.getNumberReturned();
      numberSkipped += metaResult.getNumberSkipped().orElse(0);
      lastTable = currentTable;
      lastNumberReturned = metaResult.getNumberReturned();
      lastNumberSkipped = metaResult.getNumberSkipped().orElse(0);
    }
  }

  // TODO: simplify, class SqlQueryRunner, remove options, singleFeature
  @Override
  default Reactive.Source<SqlRow> getSourceStream(
      SqlQueryBatch queryBatch, SqlQueryOptions options) {
    Paging paging =
        new Paging(
            queryBatch.getLimit(),
            queryBatch.getOffset(),
            queryBatch.getChunkSize(),
            queryBatch.isAllowSkipMetaQueries());

    Source<SqlRow> sqlRowSource1 =
        Source.iterable(queryBatch.getQuerySets())
            .via(
                Transformer.flatMap(
                    querySet -> {
                      String currentTable = querySet.getTableSchemas().get(0).getFullPathAsString();

                      Optional<Tuple<Long, Long>> maxLimitAndSkipped = paging.get(currentTable);

                      if (maxLimitAndSkipped.isEmpty()) {
                        return Source.empty();
                      }

                      return getMetaResult(
                              querySet
                                  .getMetaQuery()
                                  .apply(
                                      maxLimitAndSkipped.get().first(),
                                      maxLimitAndSkipped.get().second()),
                              options,
                              currentTable)
                          .via(
                              Transformer.map(
                                  metaResult -> {
                                    paging.register(currentTable, metaResult);

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
                      Paging paging2 =
                          new Paging(
                              queryBatch.getLimit(),
                              queryBatch.getOffset(),
                              queryBatch.getChunkSize(),
                              queryBatch.isAllowSkipMetaQueries());
                      int[] i = {0};

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
                                        querySets
                                            .get(index)
                                            .getTableSchemas()
                                            .get(0)
                                            .getFullPathAsString();
                                    int[] j = {0};

                                    if (metaResults.get(index).getNumberReturned() <= 0) {
                                      paging2.register(currentTable, metaResults.get(index));
                                      return Source.empty();
                                    }

                                    Optional<Tuple<Long, Long>> maxLimitAndSkipped =
                                        paging2.get(currentTable);

                                    Source<SqlRow>[] sqlRows =
                                        querySets
                                            .get(index)
                                            .getValueQueries()
                                            .apply(
                                                metaResults.get(index),
                                                maxLimitAndSkipped.get().first(),
                                                maxLimitAndSkipped.get().second())
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
                                                                        .get(j[0]++))
                                                                .type(
                                                                    querySets
                                                                        .get(index)
                                                                        .getOptions()
                                                                        .getType())
                                                                .containerPriority(i[0]++)
                                                                .build()))
                                            .toArray((IntFunction<Source<SqlRow>[]>) Source[]::new);

                                    paging2.register(currentTable, metaResults.get(index));

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
        ImmutableList.of(Object.class, Object.class, Long.class, Long.class, Long.class);

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

    for (int i = 0; i < size - 5; i += 2) {
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
                        if (!"0".equals(msg)) {
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
