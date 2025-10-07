/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.features.domain.FeatureProviderCapabilities;
import de.ii.xtraplatform.features.domain.FeatureProviderCapabilities.Level;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.FeatureQueryEncoder;
import de.ii.xtraplatform.features.domain.ImmutableFeatureProviderCapabilities;
import de.ii.xtraplatform.features.domain.ImmutableSortKey;
import de.ii.xtraplatform.features.domain.MultiFeatureQuery;
import de.ii.xtraplatform.features.domain.Query;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SortKey;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.domain.TypeQuery;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData.QueryGeneratorSettings;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryBatch;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQuerySet;
import de.ii.xtraplatform.features.sql.domain.SchemaSql.PropertyTypeInfo;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlQueryBatch;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySet;
import de.ii.xtraplatform.features.sql.domain.SqlRowMeta;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.function.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureQueryEncoderSql implements FeatureQueryEncoder<SqlQueryBatch, SqlQueryOptions> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureQueryEncoderSql.class);

  private final Map<String, List<SqlQueryTemplates>> allQueryTemplates;
  private final Map<String, List<SqlQueryTemplates>> allQueryTemplatesMutations;
  private final int chunkSize;
  private final boolean skipRedundantMetaQueries;
  private final SqlDialect sqlDialect;
  private final boolean geometryAsWkb;

  public FeatureQueryEncoderSql(
      Map<String, List<SqlQueryTemplates>> allQueryTemplates,
      Map<String, List<SqlQueryTemplates>> allQueryTemplatesMutations,
      QueryGeneratorSettings queryGeneratorSettings,
      SqlDialect sqlDialect) {
    this.allQueryTemplates = allQueryTemplates;
    this.allQueryTemplatesMutations = allQueryTemplatesMutations;
    this.chunkSize = queryGeneratorSettings.getChunkSize();
    this.skipRedundantMetaQueries = !queryGeneratorSettings.getComputeNumberMatched();
    this.geometryAsWkb = queryGeneratorSettings.getGeometryAsWkb();
    this.sqlDialect = sqlDialect;
  }

  // TODO: add cql2 classes
  @Override
  public FeatureProviderCapabilities getCapabilities() {
    return ImmutableFeatureProviderCapabilities.builder().level(Level.FULL).build();
  }

  // TODO: cleanup options
  @Override
  public SqlQueryBatch encode(Query query, Map<String, String> additionalQueryParameters) {
    if (query instanceof FeatureQuery) {
      return encode((FeatureQuery) query, additionalQueryParameters);
    }

    if (query instanceof MultiFeatureQuery) {
      return encode((MultiFeatureQuery) query, additionalQueryParameters);
    }

    throw new IllegalArgumentException();
  }

  private SqlQueryBatch encode(FeatureQuery query, Map<String, String> additionalQueryParameters) {
    List<SqlQueryTemplates> queryTemplates =
        query.getSchemaScope() == SchemaBase.Scope.RETURNABLE
            ? allQueryTemplates.get(query.getType())
            : allQueryTemplatesMutations.get(query.getType());
    int chunks =
        query.returnsSingleFeature()
            ? 1
            : (query.getLimit() / chunkSize) + (query.getLimit() % chunkSize > 0 ? 1 : 0);

    List<SqlQuerySet> querySets =
        IntStream.range(0, queryTemplates.size())
            .mapToObj(
                tableIndex ->
                    IntStream.range(0, chunks)
                        .mapToObj(
                            chunk ->
                                createQuerySet(
                                    queryTemplates.get(tableIndex),
                                    chunkSize,
                                    query.getOffset() + (chunk * chunkSize),
                                    chunk,
                                    query,
                                    query,
                                    additionalQueryParameters,
                                    query.returnsSingleFeature())))
            .flatMap(s -> s)
            .collect(Collectors.toList());

    // reuse SqlQuerySet instances instead of copying them; this is expensive and unnecessary, since
    // they are immutable
    return new ImmutableSqlQueryBatch.Builder()
        .limit(query.getLimit())
        .offset(query.getOffset())
        .chunkSize(chunkSize)
        .isSingleFeature(query.returnsSingleFeature())
        .isAllowSkipMetaQueries(skipRedundantMetaQueries)
        .build()
        .withQuerySets(querySets);
  }

  private SqlQueryBatch encode(
      MultiFeatureQuery query, Map<String, String> additionalQueryParameters) {
    int chunks = (query.getLimit() / chunkSize) + (query.getLimit() % chunkSize > 0 ? 1 : 0);

    List<SqlQuerySet> querySets =
        query.getQueries().stream()
            .flatMap(
                typeQuery -> {
                  List<SqlQueryTemplates> queryTemplates =
                      allQueryTemplates.get(typeQuery.getType());

                  return IntStream.range(0, queryTemplates.size())
                      .mapToObj(
                          tableIndex ->
                              IntStream.range(0, chunks)
                                  .mapToObj(
                                      chunk ->
                                          createQuerySet(
                                              queryTemplates.get(tableIndex),
                                              chunkSize,
                                              query.getOffset() + (chunk * chunkSize),
                                              chunk,
                                              typeQuery,
                                              query,
                                              additionalQueryParameters,
                                              false)))
                      .flatMap(s -> s);
                })
            .collect(Collectors.toList());

    // reuse SqlQuerySet instances instead of copying them; this is expensive and unnecessary, since
    // they are immutable
    return new ImmutableSqlQueryBatch.Builder()
        .limit(query.getLimit())
        .offset(query.getOffset())
        .chunkSize(chunkSize)
        .build()
        .withQuerySets(querySets);
  }

  private SqlQuerySet createQuerySet(
      SqlQueryTemplates queryTemplates,
      long limit,
      long offset,
      int chunk,
      TypeQuery typeQuery,
      Query query,
      Map<String, String> additionalQueryParameters,
      boolean skipMetaQuery) {
    List<SortKey> sortKeys =
        transformSortKeys(typeQuery.getSortKeys(), queryTemplates.getMapping());

    BiFunction<Long, Long, Optional<String>> metaQuery =
        (maxLimit, skipped) ->
            skipMetaQuery
                ? Optional.empty()
                : Optional.of(
                    queryTemplates
                        .getMetaQueryTemplate()
                        .generateMetaQuery(
                            Math.min(limit, maxLimit),
                            Math.max(0L, offset - skipped),
                            chunk * limit,
                            sortKeys,
                            typeQuery.getFilter(),
                            additionalQueryParameters,
                            query.getOffset() > 0,
                            maxLimit > 0 && !query.hitsOnly(),
                            query.hitsOnly()));

    TriFunction<SqlRowMeta, Long, Long, Stream<String>> valueQueries =
        (metaResult, maxLimit, skipped) ->
            queryTemplates.getValueQueryTemplates().stream()
                .map(
                    valueQueryTemplate ->
                        valueQueryTemplate.generateValueQuery(
                            Math.min(limit, maxLimit),
                            Math.max(0L, offset - skipped),
                            sortKeys,
                            typeQuery.getFilter(),
                            typeQuery.forceSimpleFeatureGeometry(),
                            ((Objects.nonNull(metaResult.getMinKey())
                                        && Objects.nonNull(metaResult.getMaxKey()))
                                    || metaResult.getNumberReturned() == 0)
                                ? Optional.of(
                                    Tuple.of(metaResult.getMinKey(), metaResult.getMaxKey()))
                                : Optional.empty(),
                            additionalQueryParameters));

    // reuse SchemaSql instances instead of copying them; this is expensive and unnecessary, since
    // they are immutable
    return new ImmutableSqlQuerySet.Builder()
        .metaQuery(metaQuery)
        .valueQueries(valueQueries)
        .options(getOptions(typeQuery, query))
        .build()
        .withTableSchemas(queryTemplates.getMapping().getTables());
  }

  @Override
  public SqlQueryOptions getOptions(TypeQuery typeQuery, Query query) {
    // TODO: either pass as parameter, or check for null here
    SqlQueryTemplates queryTemplates = allQueryTemplates.get(typeQuery.getType()).get(0);

    List<SortKey> sortKeys =
        transformSortKeys(typeQuery.getSortKeys(), queryTemplates.getMapping());

    return new ImmutableSqlQueryOptions.Builder()
        .type(typeQuery.getType())
        .customSortKeys(sortKeys)
        .isHitsOnly(query.hitsOnly())
        .isGeometryWkb(geometryAsWkb)
        .build();
  }

  private List<SortKey> transformSortKeys(List<SortKey> sortKeys, SqlQueryMapping mapping) {
    return sortKeys.stream()
        .map(
            sortKey -> {
              Optional<de.ii.xtraplatform.base.domain.util.Tuple<SqlQuerySchema, SqlQueryColumn>>
                  column = mapping.getColumnForValue(sortKey.getField());

              if (column.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(
                        "Sort key is invalid, property '%s' is either unknown or inapplicable.",
                        sortKey.getField()));
              }

              String columnName = column.get().second().getName();

              return ImmutableSortKey.builder().from(sortKey).field(columnName).build();
            })
        .collect(ImmutableList.toImmutableList());
  }

  private boolean typeIsSortable(PropertyTypeInfo typeInfo) {
    if (typeInfo.getInArray()) {
      return false;
    }
    Type t = typeInfo.getType();
    if (typeInfo.getType() == Type.VALUE) {
      t = typeInfo.getValueType().orElse(Type.STRING);
    }
    return t == Type.STRING || t == Type.INTEGER || t == Type.FLOAT || t == Type.FEATURE_REF;
  }
}
