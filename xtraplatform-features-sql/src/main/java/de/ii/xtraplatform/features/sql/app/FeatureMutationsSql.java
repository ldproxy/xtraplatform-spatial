/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.domain.SqlClient;
import de.ii.xtraplatform.features.sql.domain.SqlQueryJoin;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Transformer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureMutationsSql {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureMutationsSql.class);

  private final Supplier<SqlClient> sqlClient;
  private final FeatureStoreInsertGenerator generator;

  public FeatureMutationsSql(Supplier<SqlClient> sqlClient, FeatureStoreInsertGenerator generator) {
    this.sqlClient = sqlClient;
    this.generator = generator;
  }

  public Reactive.Transformer<FeatureDataSql, String> getCreatorFlow(
      SqlQueryMapping schema, Object executionContext, EpsgCrs crs) {

    RowCursor rowCursor = new RowCursor(schema.getMainTable().getFullPath());

    return sqlClient
        .get()
        .getMutationFlow(
            feature -> createInstanceInserts(feature, rowCursor, Optional.empty(), crs),
            executionContext,
            Optional.empty());
  }

  public Reactive.Transformer<FeatureDataSql, String> getUpdaterFlow(
      SqlQueryMapping schema, Object executionContext, String id, EpsgCrs crs) {

    RowCursor rowCursor = new RowCursor(schema.getMainTable().getFullPath());

    return sqlClient
        .get()
        .getMutationFlow(
            feature -> createInstanceInserts(feature, rowCursor, Optional.of(id), crs),
            executionContext,
            Optional.of(id));
  }

  public Reactive.Source<String> getDeletionSource(SqlQueryMapping mapping, String id) {
    Tuple<String, Consumer<String>> delete = createInstanceDelete(mapping, id);

    return sqlClient
        .get()
        .getSourceStream(delete.first(), SqlQueryOptions.withColumnTypes(String.class))
        .via(Transformer.map(sqlRow -> (String) sqlRow.getValues().get(0)));
  }

  List<Tuple<String, Consumer<String>>> createInstanceInserts(
      FeatureDataSql feature, RowCursor rowCursor, Optional<String> id, EpsgCrs crs) {
    Map<List<String>, List<Integer>> rowNesting = Map.of(); // TODO feature.getRowCounts();
    boolean withId = id.isPresent();

    Stream<Tuple<String, Consumer<String>>> instance =
        withId
            ? Stream.concat(
                Stream.of(createInstanceDelete(feature.getMapping(), id.get())),
                createObjectInserts(feature, rowNesting, rowCursor, id, crs).stream())
            : createObjectInserts(feature, rowNesting, rowCursor, id, crs).stream();

    return instance.toList();
    /*return Stream.concat(
        instance,
        schema.getProperties().stream()
            .filter(SchemaSql::isObject)
            .flatMap(
                childSchema ->
                    createInstanceInserts(
                        childSchema, feature, rowCursor, Optional.empty(), crs)
                        .stream()))
    .collect(Collectors.toList());*/
  }

  // TODO: to InsertGenerator
  Tuple<String, Consumer<String>> createInstanceDelete(SqlQueryMapping mapping, String id) {

    String table = mapping.getMainTable().getName();
    String idColumn =
        mapping
            .getColumnForId()
            .map(col -> col.second().getName())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No property with role ID found for '"
                            + mapping.getMainSchema().getName()
                            + "'."));

    return Tuple.of(
        String.format("DELETE FROM %s WHERE %s=%s RETURNING %2$s", table, idColumn, id),
        ignore -> {});
  }

  List<Tuple<String, Consumer<String>>> createObjectInserts(
      FeatureDataSql feature,
      Map<List<String>, List<Integer>> rowNesting,
      RowCursor rowCursor,
      Optional<String> id,
      EpsgCrs crs) {

    SqlQuerySchema schema = feature.getMapping().getMainTable();

    if (schema.getRelations().isEmpty()) {
      return createAttributesInserts(schema, feature, rowCursor.get(schema.getFullPath()), id, crs);
    }

    SqlQueryJoin relation = schema.getRelations().get(0);

    String parentName = relation.getName();

    throw new IllegalStateException("Not implemented yet");

    /*if (!relation.isM2N() && !relation.isOne2N()) {
      List<Integer> newParentRows =
          rowCursor.track(schema.getFullPath(), schema.getParentPath(), 0);

      return createAttributesInserts(schema, feature, newParentRows, id, crs);
    }

    // TODO: what are the keys?
    // TODO: err
    if (!rowNesting.containsKey(schema.getFullPath())) {
      // throw new IllegalStateException();
      return ImmutableList.of();
    }

    // TODO: nested m:n test
    List<Integer> numberOfRowsPerParentRow = rowNesting.get(schema.getFullPath()); // [1,2] | [2]
    int currentParentRow = rowCursor.getCurrent(schema.getParentPath()); // 0|1 | 0
    int rowCount = numberOfRowsPerParentRow.get(currentParentRow); // 1|2 | 2

    return IntStream.range(0, rowCount)
        .mapToObj(
            currentRow -> {
              List<Integer> newParentRows =
                  rowCursor.track(schema.getFullPath(), schema.getParentPath(), currentRow);

              return createAttributesInserts(schema, feature, newParentRows, id, crs);
            })
        .flatMap(List::stream)
        .collect(Collectors.toList());*/
  }

  List<Tuple<String, Consumer<String>>> createAttributesInserts(
      SqlQuerySchema schema,
      FeatureDataSql feature,
      List<Integer> parentRows,
      Optional<String> id,
      EpsgCrs crs) {

    ImmutableList.Builder<Tuple<String, Consumer<String>>> queries = ImmutableList.builder();

    queries.add(generator.createInsert(feature, parentRows, id, crs));

    /*if (!schema.getRelations().isEmpty()) {
      SqlQueryJoin relation = schema.getRelations().get(0);

      if (relation.isM2N()) {
        queries.add(generator.createJunctionInsert(feature, parentRows));
      }

      boolean isOne2OneWithForeignKey =
          relation.isOne2One()
              && !Objects.equals(
                  relation.getSourceSortKey().orElse("id"), relation.getSourceField());

      if (isOne2OneWithForeignKey) {
        queries.add(generator.createForeignKeyUpdate(feature, parentRows));
      }
    }*/

    return queries.build();
  }
}
