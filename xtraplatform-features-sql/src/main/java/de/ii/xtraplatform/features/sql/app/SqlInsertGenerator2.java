/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.domain.SqlPathDefaults;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation;
import de.ii.xtraplatform.features.sql.domain.SqlQueryJoin;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zahnen
 */
public class SqlInsertGenerator2 implements FeatureStoreInsertGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlInsertGenerator2.class);

  private final EpsgCrs nativeCrs;
  private final CrsTransformerFactory crsTransformerFactory;
  private final SqlPathDefaults sqlOptions;

  public SqlInsertGenerator2(
      EpsgCrs nativeCrs, CrsTransformerFactory crsTransformerFactory, SqlPathDefaults sqlOptions) {
    this.nativeCrs = nativeCrs;
    this.crsTransformerFactory = crsTransformerFactory;
    this.sqlOptions = sqlOptions;
  }

  SqlPathDefaults getSqlOptions() {
    return sqlOptions;
  }

  @Override
  public Supplier<Tuple<String, Consumer<String>>> createInsert(
      FeatureDataSql feature,
      SqlQuerySchema schema,
      List<Integer> parentRows,
      Optional<String> id,
      EpsgCrs crs) {
    SqlQueryMapping mapping = feature.getMapping();

    Optional<SqlQueryJoin> parentRelation = schema.getRelations().stream().findFirst();

    Optional<SqlQueryColumn> idProperty =
        mapping
            .getColumnForId()
            .map(de.ii.xtraplatform.base.domain.util.Tuple::second); // schema.getIdProperty();

    Map<String, String> valueOverrides = new LinkedHashMap<>();

    if (idProperty.isPresent() && id.isPresent()) {
      valueOverrides.put(
          idProperty.get().getName(),
          idProperty.get().getType() == Type.STRING ? String.format("'%s'", id.get()) : id.get());
    }

    // TODO: id instead of primaryKey if isPresent
    String primaryKey = schema.getPrimaryKey().orElse(sqlOptions.getPrimaryKey());

    Set<String> columns0 =
        schema.getWritableColumns().stream()
            // TODO: filter out mutations.ignore=true
            // TODO: filter out primaryKey if not mutations.ignore=false
            .filter(col -> !Objects.equals(col.getName(), primaryKey))
            // TODO: in deriver
            .filter(col -> !col.hasOperation(Operation.CONSTANT))
            .map(SqlQueryColumn::getName)
            .collect(ImmutableSet.toImmutableSet());

    // TODO: add id if present
    Set<String> columns =
        idProperty.isPresent() && id.isPresent()
            ? ImmutableSet.<String>builder()
                .add(idProperty.get().getName())
                .addAll(columns0)
                .build()
            : columns0;

    // TODO: from Syntax
    List<String> columns2 =
        Stream.concat(columns.stream() /*.map(
            col ->
                col.startsWith("ST_AsText(ST_ForcePolygonCCW(")
                    ? col.substring("ST_AsText(ST_ForcePolygonCCW(".length(), col.length() - 2)
                    : col)*/, schema.getStaticInserts().keySet().stream())
            .collect(Collectors.toList());

    List<String> sortKeys = new ArrayList<>();

    if (parentRelation.isPresent()) {
      // TODO: is this merged?
      if (schema.isOne2One()
          && Objects.equals(
              parentRelation.get().getSortKey(), parentRelation.get().getSourceField())) {
        // TODO fullPath, sortKey
        sortKeys.add(
            0,
            String.format(
                "%s.%s", parentRelation.get().getName(), parentRelation.get().getSortKey()));
        if (!columns2.contains(primaryKey)) {
          columns2.add(0, primaryKey);
        }

      } else if (schema.isOne2N()) {
        sortKeys.add(
            0,
            String.format(
                "%s.%s", parentRelation.get().getName(), parentRelation.get().getSourceField()));
        columns2.add(0, parentRelation.get().getTargetField());
      }
    }

    String tableName = schema.getName();
    String columnNames = Joiner.on(',').skipNulls().join(columns2);
    if (!columnNames.isEmpty()) {
      columnNames = "(" + columnNames + ")";
    }
    String finalColumnNames = columnNames;

    String returningValue =
        " RETURNING "
            + (parentRelation.isPresent() && schema.isOne2N()
                ? " null"
                : idProperty.isPresent() ? idProperty.get().getName() : primaryKey);
    Optional<String> returningName =
        parentRelation.isPresent() && schema.isOne2N()
            ? Optional.empty()
            : idProperty.isPresent()
                ? Optional.of(tableName + "." + idProperty.get().getName())
                : Optional.of(tableName + "." + primaryKey);
    boolean returningNeedsQuotes =
        idProperty.isPresent()
            && (idProperty.get().getType() == SchemaBase.Type.STRING
                || idProperty.get().getType() == SchemaBase.Type.DATETIME
                || idProperty.get().getType() == SchemaBase.Type.DATE);

    Optional<SqlRowData> parentRow =
        feature.getRow(schema.getParentPath(), parentRows.subList(0, 1));
    Optional<SqlRowData> currentRow = feature.getRow(schema.getFullPath(), parentRows);

    if (currentRow.isEmpty() || currentRow.get().isEmpty()) {
      return () -> Tuple.of(null, null);
    }

    // TODO: crs can be null, refactor FeatureProviderSql2.createFeatures
    Optional<CrsTransformer> crsTransformer =
        Objects.nonNull(crs)
            ? crsTransformerFactory.getTransformer(crs, nativeCrs)
            : Optional.empty();

    return () -> {

      // TODO: pass id to getValues if given
      String values =
          getColumnValues(
              sortKeys,
              columns,
              currentRow.get().getValues(), // TODO .getValues(crsTransformer, nativeCrs),
              currentRow.get().getIds(),
              parentRow.isPresent() ? parentRow.get().getIds() : Map.of(),
              valueOverrides,
              schema.getStaticInserts());

      if (!values.isEmpty()) {
        values = "VALUES (" + values + ")";
      } else {
        values = "DEFAULT VALUES";
      }

      String query =
          String.format(
              "INSERT INTO %s %s %s%s;", tableName, finalColumnNames, values, returningValue);

      Consumer<String> idConsumer =
          returningName
              .map(
                  name ->
                      (Consumer<String>)
                          returned -> {
                            String value =
                                returningNeedsQuotes && Objects.nonNull(returned)
                                    ? String.format("'%s'", returned.replaceAll("'", "''"))
                                    : returned;

                            currentRow.get().putIds(name, value);
                          })
              .orElse(returned -> {});

      return Tuple.of(query, idConsumer);
    };
  }

  @Override
  public Supplier<Tuple<String, Consumer<String>>> createJunctionInsert(
      FeatureDataSql feature, SqlQuerySchema schema, List<Integer> parentRows) {

    if (!schema.isM2N()) {
      throw new IllegalArgumentException();
    }

    List<SqlQueryJoin> joins = schema.getRelations();

    String table = joins.get(0).getTarget();
    String columnNames =
        String.format("%s,%s", joins.get(0).getTargetField(), joins.get(1).getSourceField());
    String sourceIdColumn =
        String.format("%s.%s", joins.get(0).getName(), joins.get(0).getSourceField());
    String targetIdColumn =
        String.format("%s.%s", joins.get(1).getTarget(), joins.get(1).getTargetField());

    Optional<SqlRowData> parentRow =
        feature.getRow(schema.getParentPath(), parentRows.subList(0, 1));
    Optional<SqlRowData> currentRow = feature.getRow(schema.getFullPath(), parentRows);

    if (currentRow.isEmpty() || currentRow.get().isEmpty()) {
      return () -> Tuple.of(null, null);
    }

    return () -> {
      Map<String, String> parentIds = parentRow.isPresent() ? parentRow.get().getIds() : Map.of();
      Map<String, String> ids = currentRow.get().getIds();

      String sourceId =
          parentIds.containsKey(sourceIdColumn)
              ? parentIds.get(sourceIdColumn)
              : ids.get(sourceIdColumn);
      String targetId =
          schema.isJunctionReference()
              ? currentRow.get().getValues().get(joins.get(1).getTargetField())
              : ids.get(targetIdColumn);

      String columnNames2 = columnNames;
      String columnValues = String.format("%s,%s", sourceId, targetId);

      if (!joins.get(1).getStaticInserts().isEmpty()) {
        columnNames2 += "," + String.join(",", joins.get(1).getStaticInserts().keySet());
        columnValues += "," + String.join(",", joins.get(1).getStaticInserts().values());
      }

      return Tuple.of(
          String.format(
              "INSERT INTO %s (%s) VALUES (%s) RETURNING null;", table, columnNames2, columnValues),
          id -> {});
    };
  }

  @Override
  public Supplier<Tuple<String, Consumer<String>>> createForeignKeyUpdate(
      FeatureDataSql feature, SqlQuerySchema schema, List<Integer> parentRows) {

    /*TODO if (schema.getRelation().isEmpty()
        || !(schema.getRelation().get(0).isOne2One() || schema.getRelation().get(0).isOne2N())) {
      throw new IllegalArgumentException();
    }*/

    SqlQueryJoin relation = schema.getRelations().get(0);

    String table = relation.getName();
    String refKey = String.format("%s.%s", table, relation.getSourceField());
    String column = relation.getSourceField();
    String columnKey = String.format("%s.%s", relation.getTarget(), relation.getTargetField());

    Optional<SqlRowData> currentRow = feature.getRow(schema.getFullPath(), parentRows);

    if (currentRow.isEmpty() || currentRow.get().isEmpty()) {
      return () -> Tuple.of(null, null);
    }

    Map<String, String> ids = currentRow.get().getIds();

    return () ->
        Tuple.of(
            String.format(
                "UPDATE %s SET %s=%s WHERE id=%s RETURNING null;",
                table, column, ids.get(columnKey), ids.get(refKey)),
            id -> {});
  }

  // TODO: from syntax
  // TODO: separate column and id column names
  String getColumnValues(
      List<String> idKeys,
      Set<String> columnNames,
      Map<String, String> values,
      Map<String, String> ids,
      Map<String, String> parentIds,
      Map<String, String> valueOverrides,
      Map<String, String> staticInserts) {

    return Stream.concat(
            Stream.concat(
                idKeys.stream()
                    .map(key -> parentIds.containsKey(key) ? parentIds.get(key) : ids.get(key)),
                columnNames.stream()
                    .map(
                        name -> {
                          // TODO: value transformer?
                          if (name.startsWith("ST_AsText(ST_ForcePolygonCCW(")) {
                            return String.format(
                                "ST_ForcePolygonCW(ST_GeomFromText(%s,25832))",
                                values.get(name)); // TODO srid from config
                          }
                          if (valueOverrides.containsKey(name)) {
                            return valueOverrides.get(name);
                          }
                          return values.get(name);
                        })),
            staticInserts.values().stream())
        .collect(Collectors.joining(","));
  }
}
