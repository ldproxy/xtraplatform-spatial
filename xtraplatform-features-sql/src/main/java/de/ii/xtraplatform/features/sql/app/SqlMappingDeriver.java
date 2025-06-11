/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import static de.ii.xtraplatform.features.domain.MappingRule.ROOT_TARGET;
import static de.ii.xtraplatform.features.sql.domain.SqlQueryMapping.IN_CONNECTED_ARRAY;
import static de.ii.xtraplatform.features.sql.domain.SqlQueryMapping.PATH_IN_CONNECTOR;

import de.ii.xtraplatform.cql.domain.Operation;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.MappingOperationResolver;
import de.ii.xtraplatform.features.domain.MappingRule;
import de.ii.xtraplatform.features.domain.MappingRulesDeriver;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformation;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData.QueryGeneratorSettings;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryJoin;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQuerySchema;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQuerySchema.Builder;
import de.ii.xtraplatform.features.sql.domain.SqlPath;
import de.ii.xtraplatform.features.sql.domain.SqlPath.JoinType;
import de.ii.xtraplatform.features.sql.domain.SqlPathParser;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryJoin;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class SqlMappingDeriver {

  private final SqlPathParser pathParser;
  private final QueryGeneratorSettings queryGeneration;

  public SqlMappingDeriver(SqlPathParser pathParser, QueryGeneratorSettings queryGeneration) {
    this.pathParser = pathParser;
    this.queryGeneration = queryGeneration;
  }

  public List<SqlQueryMapping> derive(List<MappingRule> mappingRules, FeatureSchema schema) {
    List<SqlQuerySchema> schemas = new ArrayList<>();
    List<List<String>> previous = new ArrayList<>();
    List<String> seenProperties = new ArrayList<>();
    List<String> seenWritableProperties = new ArrayList<>();
    boolean includeSchema = Objects.nonNull(schema);
    int i = 0;

    List<SqlQueryMapping> mappings = new ArrayList<>();
    ImmutableSqlQueryMapping.Builder mapping = null;

    while (i < mappingRules.size()) {
      MappingRule rule = mappingRules.get(i);

      if (!pathParser.isTablePath(rule.getSource())) {
        i++;
        continue;
      }

      if (pathParser.isRootPath(rule.getSource())) {
        if (Objects.nonNull(mapping)) {
          mappings.add(mapping.build());
        }

        mapping = new ImmutableSqlQueryMapping.Builder();

        if (includeSchema) {
          mapping.mainSchema(schema);
        }
      }

      int j = i + 1;
      List<MappingRule> columnRules = new ArrayList<>();
      List<MappingRule> filterColumnRules = new ArrayList<>();
      List<MappingRule> writableColumnRules = new ArrayList<>();

      if (j >= mappingRules.size()) {
        break;
      }

      while (mappingRules.get(j).getSource().startsWith(rule.getSource())) {
        MappingRule columnRule = mappingRules.get(j);

        if (pathParser.isTablePath(columnRule.getSource())) {
          if (i == 0 && !columnRule.getSource().substring(1).contains("/")) {
            j++;
            continue;
          }
          break;
        }

        if (columnRule.isFilterOnly()) {
          filterColumnRules.add(columnRule);
        } else {
          if (columnRule.isReadable()) {
            columnRules.add(columnRule);
          }
          if (columnRule.isWritable()) {
            writableColumnRules.add(columnRule);
          }
        }

        j++;
        if (j >= mappingRules.size()) {
          break;
        }
      }

      if (j == i + 1) {
        i++;
        continue;
      }

      addToMapping(
          schema,
          rule,
          columnRules,
          filterColumnRules,
          writableColumnRules,
          previous,
          schemas,
          mapping,
          seenProperties,
          seenWritableProperties,
          includeSchema);

      i = j;
    }

    if (Objects.nonNull(mapping)) {
      mappings.add(mapping.build());
    }

    return mappings;
  }

  private void addToMapping(
      FeatureSchema schema,
      MappingRule tableRule,
      List<MappingRule> columnRules,
      List<MappingRule> filterColumnRules,
      List<MappingRule> writableColumnRules,
      List<List<String>> previous,
      List<SqlQuerySchema> schemas,
      ImmutableSqlQueryMapping.Builder mapping,
      List<String> seenProperties,
      List<String> seenWritableProperties,
      boolean includeSchema) {
    SqlQuerySchema querySchema =
        derive(schema, tableRule, columnRules, filterColumnRules, writableColumnRules, previous);

    schemas.add(querySchema);
    previous.add(pathParser.parseTablePath(tableRule.getSource()).getFullPath());
    mapping.addTables(querySchema);

    if (tableRule.isWritable()
        && !seenWritableProperties.contains(tableRule.getTarget())
        && !Objects.equals(tableRule.getTarget(), ROOT_TARGET)) {
      mapping.putObjectTables(tableRule.getTarget(), querySchema);
      seenWritableProperties.add(tableRule.getTarget());
    }

    for (int k = 0; k < columnRules.size(); k++) {
      MappingRule column = columnRules.get(k);
      if (seenProperties.contains(column.getTarget())) {
        continue;
      }
      SqlQueryColumn column1 = querySchema.getColumns().get(k);

      addToMapping(
          schema,
          mapping,
          seenProperties,
          seenWritableProperties,
          includeSchema,
          column,
          column1,
          querySchema,
          false);
    }

    for (int k = 0; k < filterColumnRules.size(); k++) {
      MappingRule column = filterColumnRules.get(k);
      if (seenProperties.contains(column.getTarget())) {
        continue;
      }
      SqlQueryColumn column1 = querySchema.getFilterColumns().get(k);

      addToMapping(
          schema,
          mapping,
          seenProperties,
          seenWritableProperties,
          includeSchema,
          column,
          column1,
          querySchema,
          false);
    }

    for (int k = 0; k < writableColumnRules.size(); k++) {
      MappingRule column = writableColumnRules.get(k);
      if (seenWritableProperties.contains(column.getTarget())) {
        continue;
      }
      SqlQueryColumn column1 = querySchema.getWritableColumns().get(k);

      // ignoring joins for now
      if (!Objects.equals(ROOT_TARGET, tableRule.getTarget())) {
        continue;
      }

      if (column1.hasOperation(SqlQueryColumn.Operation.CONSTANT)) {
        continue;
      }

      addToMapping(
          schema,
          mapping,
          seenProperties,
          seenWritableProperties,
          includeSchema,
          column,
          column1,
          querySchema,
          true);
    }
  }

  private void addToMapping(
      FeatureSchema schema,
      ImmutableSqlQueryMapping.Builder mapping,
      List<String> seenProperties,
      List<String> seenWritableProperties,
      boolean includeSchema,
      MappingRule column,
      SqlQueryColumn column1,
      SqlQuerySchema querySchema,
      boolean isWritable) {
    if (column.getTarget().equals("$")) {
      if (column1.hasOperation(SqlQueryColumn.Operation.CONNECTOR)) {
        List<FeatureSchema> connectedSchemas =
            includeSchema
                ? getConnectedSchemas(schema, column1.getPathSegment(), "", false)
                : List.of();

        for (FeatureSchema p : connectedSchemas) {
          if (isWritable) {
            mapping.putWritableTables(p.getFullPathAsString(), querySchema);
            mapping.putWritableColumns(p.getFullPathAsString(), column1);
            seenWritableProperties.add(p.getFullPathAsString());
          }
          if (!seenProperties.contains(p.getFullPathAsString())) {
            mapping.putValueTables(p.getFullPathAsString(), querySchema);
            mapping.putValueColumns(p.getFullPathAsString(), column1);
            mapping.putValueSchemas(p.getFullPathAsString(), p);
            seenProperties.add(p.getFullPathAsString());
          }
        }
      }
    } else {
      String target = column.getTarget();
      boolean handleSchema = includeSchema && !MappingRulesDeriver.doIgnore(column.getTarget());
      FeatureSchema propertySchema = null;

      if (handleSchema) {
        propertySchema =
            schema.getAllNestedProperties().stream()
                .filter(property -> matches(column, property))
                .findFirst()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Schema not found for property: " + column.getIdentifier()));

        target = applyRename(column.getTarget(), propertySchema);

        if (!seenProperties.contains(target)) {
          mapping.putValueSchemas(target, propertySchema);
        }

        /*if (!Objects.equals(target, column.getTarget())) {
          System.out.println(
              "RENAMING target from "
                  + column.getTarget()
                  + " to "
                  + target
                  + " for column: "
                  + column.getSource());
        }*/
      }

      if (isWritable) {
        mapping.putWritableTables(target, querySchema);
        mapping.putWritableColumns(target, column1);
        seenWritableProperties.add(target);
      }
      if (!seenProperties.contains(target)) {
        mapping.putValueTables(target, querySchema);
        mapping.putValueColumns(target, column1);
        seenProperties.add(target);
      }
    }
  }

  private static String applyRename(String target, FeatureSchema schema) {
    if (Objects.nonNull(schema)) {
      Optional<String> rename =
          schema.getTransformations().stream()
              .filter(t -> t.getRename().isPresent())
              .findFirst()
              .flatMap(PropertyTransformation::getRename);

      if (rename.isPresent()) {
        return applyRename(target, rename.get());
      }
    }
    return target;
  }

  private static String applyRename(String target, String rename) {
    String prefix = target.contains(".") ? target.substring(0, target.lastIndexOf(".") + 1) : "";
    String prop = target.contains(".") ? target.substring(target.lastIndexOf(".") + 1) : "";
    String renamed = rename;

    if (MappingOperationResolver.isConcatPath(prop)) {
      renamed = prop.substring(0, prop.indexOf("_") + 1) + renamed;
    }

    return prefix + renamed;
  }

  private List<FeatureSchema> getConnectedSchemas(
      FeatureSchema parent, String connector, String pathInConnector, boolean inArray) {
    return parent.getProperties().stream()
        .filter(
            p ->
                !p.getEffectiveSourcePaths().isEmpty()
                    && (Objects.isNull(connector)
                        || p.getEffectiveSourcePaths().get(0).startsWith(connector)))
        .flatMap(
            p -> {
              String path = p.getEffectiveSourcePaths().get(0);

              String newPathInConnector =
                  pathInConnector.isEmpty()
                      ? path.replace(connector + "/", "").replace(connector, "")
                      : pathInConnector + "." + path;

              FeatureSchema schema =
                  !inArray && pathInConnector.isEmpty()
                      ? p
                      : new ImmutableFeatureSchema.Builder()
                          .from(p)
                          .putAdditionalInfo(IN_CONNECTED_ARRAY, String.valueOf(inArray))
                          .putAdditionalInfo(PATH_IN_CONNECTOR, newPathInConnector)
                          .build();

              if (p.isValue()) {
                return Stream.of(schema);
              }

              return Stream.concat(
                  Stream.of(schema),
                  getConnectedSchemas(p, null, newPathInConnector, inArray || p.isArray())
                      .stream());
            })
        .toList();
  }

  private SqlQuerySchema derive(
      FeatureSchema schema,
      MappingRule table,
      List<MappingRule> columns,
      List<MappingRule> filterColumnRules,
      List<MappingRule> writableColumnRules,
      List<List<String>> previous) {
    SqlPath sqlPath = pathParser.parseFullTablePath(table.getSource());

    ImmutableSqlQuerySchema querySchema =
        new Builder()
            .name(sqlPath.getName())
            .pathSegment(sqlPath.asPath())
            .sortKey(sqlPath.getSortKey())
            .filter(sqlPath.getFilter().map(expr -> (Operation<?>) expr))
            .columns(columns.stream().map(column -> getColumn(schema, column)).toList())
            .filterColumns(
                filterColumnRules.stream().map(column1 -> getColumn(schema, column1)).toList())
            .writableColumns(
                writableColumnRules.stream().map(column1 -> getColumn(schema, column1)).toList())
            .relations(getJoins(sqlPath, previous))
            .staticInserts(sqlPath.getStaticInserts())
            .build();

    return querySchema;
  }

  private static List<SqlQueryJoin> getJoins(SqlPath path, List<List<String>> previous) {
    List<SqlQueryJoin> joins = new ArrayList<>();
    List<String> fullPath = new ArrayList<>();

    for (int i = 0; i < path.getParentTables().size(); i++) {
      boolean isFirst = i == 0;
      boolean isLast = i == path.getParentTables().size() - 1;
      SqlPath parentTable = path.getParentTables().get(i);
      SqlPath childTable = isLast ? path : path.getParentTables().get(i + 1);
      if (i == 0) {
        fullPath.add(parentTable.asPath());
      }
      fullPath.add(childTable.asPath());

      if (childTable.getJoin().isPresent()) {
        joins.add(
            new ImmutableSqlQueryJoin.Builder()
                .name(parentTable.getName())
                .pathSegment(parentTable.asPath())
                .sourceField(childTable.getJoin().get().first())
                .sortKey(parentTable.getSortKey())
                .filter(parentTable.getFilter().map(expr -> (Operation<?>) expr))
                .target(childTable.getName())
                .targetField(childTable.getJoin().get().second())
                .joinType(childTable.getJoinType().orElse(JoinType.INNER))
                .junction(
                    !isFirst
                        && previous.stream()
                            .noneMatch(
                                tablePath ->
                                    Objects.equals(
                                        tablePath, fullPath.subList(0, fullPath.size() - 1))))
                .staticInserts(parentTable.getStaticInserts())
                .build());
      }
    }

    return joins;
  }

  private SqlQueryColumn getColumn(FeatureSchema schema, MappingRule column) {
    SqlPath sqlPath = pathParser.parseColumnPath(column.getSource());

    Optional<FeatureSchema> propertySchema =
        Optional.ofNullable(schema)
            .flatMap(
                s ->
                    s.getAllNestedProperties().stream()
                        .filter(property -> matches(column, property))
                        .findFirst());

    return new ImmutableSqlQueryColumn.Builder()
        .name(sqlPath.getName())
        .pathSegment(sqlPath.asPath())
        .type(column.getType())
        .role(column.getRole())
        .operations(getColumnOperations(column, sqlPath, propertySchema))
        .build();
  }

  private static boolean matches(MappingRule column, FeatureSchema schema) {

    if (Objects.equals(column.getTarget(), schema.getFullPathAsString())) {
      return true;
    }

    // special handling for concat/coalesce properties
    if (!schema.getEffectiveSourcePaths().isEmpty()
        && column.getSource().endsWith(schema.getEffectiveSourcePaths().get(0))
        && MappingOperationResolver.isConcatPath(schema.getFullPathAsString())) {
      return Objects.equals(
          column.getTarget(),
          MappingOperationResolver.cleanConcatPath(schema.getFullPathAsString()));
    }

    return false;
  }

  private Map<SqlQueryColumn.Operation, String[]> getColumnOperations(
      MappingRule column, SqlPath sqlPath, Optional<FeatureSchema> propertySchema) {
    Map<SqlQueryColumn.Operation, String[]> operations = new LinkedHashMap<>();

    if (sqlPath.getConstantValue().isPresent()) {
      operations.put(
          SqlQueryColumn.Operation.CONSTANT, new String[] {sqlPath.getConstantValue().get()});
    }

    if (sqlPath.isConnected()) {
      String connector = sqlPath.getConnector().orElse("");

      operations.put(SqlQueryColumn.Operation.CONNECTOR, new String[] {connector});

      if (Objects.equals(connector, SqlQueryColumn.Operation.EXPRESSION.name())) {
        operations.put(
            SqlQueryColumn.Operation.EXPRESSION,
            new String[] {sqlPath.getPathInConnector().orElse("")});
      }
    }

    if (column.getType() == Type.GEOMETRY) {
      SqlQueryColumn.Operation op =
          queryGeneration.getGeometryAsWkb()
              ? SqlQueryColumn.Operation.WKB
              : SqlQueryColumn.Operation.WKT;

      operations.put(op, new String[] {});

      if (propertySchema.isPresent() && propertySchema.get().isForcePolygonCCW()) {
        operations.put(SqlQueryColumn.Operation.FORCE_POLYGON_CCW, new String[] {});
      }

      if (propertySchema.isPresent() && propertySchema.get().shouldLinearizeCurves()) {
        operations.put(SqlQueryColumn.Operation.LINEARIZE_CURVES, new String[] {});
      }
    }

    if (column.getType() == Type.DATETIME) {
      String[] format =
          propertySchema.isPresent() && propertySchema.get().getFormat().isPresent()
              ? new String[] {propertySchema.get().getFormat().get()}
              : new String[] {};
      operations.put(SqlQueryColumn.Operation.DATETIME, format);
    }

    if (column.getType() == Type.DATE) {
      String[] format =
          propertySchema.isPresent() && propertySchema.get().getFormat().isPresent()
              ? new String[] {propertySchema.get().getFormat().get()}
              : new String[] {};
      operations.put(SqlQueryColumn.Operation.DATE, format);
    }

    return operations;
  }
}
