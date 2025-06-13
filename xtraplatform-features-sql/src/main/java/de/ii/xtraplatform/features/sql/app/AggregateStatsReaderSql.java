/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.AggregateStatsReader;
import de.ii.xtraplatform.features.sql.domain.SqlClient;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Source;
import de.ii.xtraplatform.streams.domain.Reactive.Stream;
import de.ii.xtraplatform.streams.domain.Reactive.Transformer;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.threeten.extra.Interval;

public class AggregateStatsReaderSql implements AggregateStatsReader<SqlQueryMapping> {

  private final Supplier<SqlClient> sqlClient;
  private final AggregateStatsQueryGenerator queryGenerator;
  private final SqlDialect sqlDialect;
  private final EpsgCrs crs;

  public AggregateStatsReaderSql(
      Supplier<SqlClient> sqlClient,
      AggregateStatsQueryGenerator queryGenerator,
      SqlDialect sqlDialect,
      EpsgCrs crs) {
    this.sqlClient = sqlClient;
    this.queryGenerator = queryGenerator;
    this.sqlDialect = sqlDialect;
    this.crs = crs;
  }

  @Override
  public Stream<Long> getCount(List<SqlQueryMapping> sourceSchemas) {
    return Reactive.Source.iterable(sourceSchemas)
        .via(
            Reactive.Transformer.flatMap(
                schemaSql ->
                    sqlClient
                        .get()
                        .getSourceStream(
                            queryGenerator.getCountQuery(schemaSql), SqlQueryOptions.single())))
        .via(Reactive.Transformer.map(sqlRow -> Long.parseLong((String) sqlRow.getValues().get(0))))
        .to(Reactive.Sink.reduce(0L, Long::sum));
  }

  @Override
  public Stream<Optional<BoundingBox>> getSpatialExtent(
      List<SqlQueryMapping> sourceSchemas, boolean is3d) {
    return Source.iterable(sourceSchemas)
        .via(
            Transformer.flatMap(
                mapping -> {
                  Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> spatial =
                      mapping.getColumnForPrimaryGeometry();

                  if (spatial.isEmpty()) {
                    return Source.empty();
                  }

                  return sqlClient
                      .get()
                      .getSourceStream(
                          queryGenerator.getSpatialExtentQuery(
                              mapping, spatial.get().first(), spatial.get().second(), is3d),
                          SqlQueryOptions.single());
                }))
        .via(
            Transformer.map(
                sqlRow -> sqlDialect.parseExtent((String) sqlRow.getValues().get(0), crs)))
        .to(
            Reactive.Sink.reduce(
                Optional.empty(),
                (prev, next) -> {
                  if (next.isEmpty()) {
                    return prev;
                  }
                  if (prev.isEmpty()) {
                    return next;
                  }

                  return Optional.of(BoundingBox.merge(prev.get(), next.get()));
                }));
  }

  @Override
  public Stream<Optional<Interval>> getTemporalExtent(List<SqlQueryMapping> sourceSchemas) {
    return Source.iterable(sourceSchemas)
        .via(
            Transformer.flatMap(
                mapping -> {
                  Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> instant =
                      mapping.getColumnForPrimaryInstant();

                  if (instant.isPresent()) {
                    return sqlClient
                        .get()
                        .getSourceStream(
                            queryGenerator.getTemporalExtentQuery(
                                mapping, instant.get().first(), instant.get().second()),
                            SqlQueryOptions.tuple());
                  }
                  Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> intervalStart =
                      mapping.getColumnForPrimaryIntervalStart();
                  Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> intervalEnd =
                      mapping.getColumnForPrimaryIntervalEnd();

                  if (intervalStart.isPresent() && intervalEnd.isPresent()) {
                    return sqlClient
                        .get()
                        .getSourceStream(
                            queryGenerator.getTemporalExtentQuery(
                                mapping,
                                intervalStart.get().first(),
                                intervalStart.get().second(),
                                intervalEnd.get().first(),
                                intervalEnd.get().second()),
                            SqlQueryOptions.tuple());
                  }

                  return Source.empty();
                }))
        .via(
            Transformer.map(
                sqlRow ->
                    sqlDialect.parseTemporalExtent(
                        (String) sqlRow.getValues().get(0), (String) sqlRow.getValues().get(1))))
        .to(
            Reactive.Sink.reduce(
                Optional.empty(),
                (prev, next) -> {
                  if (next.isEmpty()) {
                    return prev;
                  }
                  if (prev.isEmpty()) {
                    return next;
                  }

                  return Optional.of(prev.get().span(next.get()));
                }));
  }
}
