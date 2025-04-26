/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import static de.ii.xtraplatform.features.sql.domain.SqlQueryMapping.IN_CONNECTED_ARRAY;
import static de.ii.xtraplatform.features.sql.domain.SqlQueryMapping.PATH_IN_CONNECTOR;

import de.ii.xtraplatform.cql.domain.Operation;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.MappingOperationResolver;
import de.ii.xtraplatform.features.domain.MappingRule;
import de.ii.xtraplatform.features.domain.MappingRulesDeriver;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class SqlMappingDeriver {

  private final SqlPathParser pathParser;
  private final QueryGeneratorSettings queryGeneration;

  public SqlMappingDeriver(SqlPathParser pathParser, QueryGeneratorSettings queryGeneration) {
    this.pathParser = pathParser;
    this.queryGeneration = queryGeneration;
  }

  public SqlQueryMapping derive(List<MappingRule> mappingRules, FeatureSchema schema) {
    List<SqlQuerySchema> schemas = new ArrayList<>();
    List<List<String>> previous = new ArrayList<>();
    List<String> seenProperties = new ArrayList<>();
    boolean includeSchema = Objects.nonNull(schema);
    int i = 0;

    ImmutableSqlQueryMapping.Builder mapping = new ImmutableSqlQueryMapping.Builder();

    if (includeSchema) {
      mapping.mainSchema(schema);
    }

    while (i < mappingRules.size()) {
      MappingRule rule = mappingRules.get(i);

      if (!pathParser.isTablePath(rule.getSource())) {
        i++;
        continue;
      }

      int j = i + 1;
      List<MappingRule> columnRules = new ArrayList<>();

      if (j >= mappingRules.size()) {
        break;
      }

      while (mappingRules.get(j).getSource().startsWith(rule.getSource())) {
        if (pathParser.isTablePath(mappingRules.get(j).getSource())) {
          if (i == 0 && !mappingRules.get(j).getSource().substring(1).contains("/")) {
            j++;
            continue;
          }
          break;
        }
        columnRules.add(mappingRules.get(j));
        j++;
        if (j >= mappingRules.size()) {
          break;
        }
      }

      if (j == i + 1) {
        i++;
        continue;
      }

      // List<MappingRule> columnRules = mappingRules.subList(i + 1, j);
      SqlQuerySchema querySchema = derive(rule, columnRules, previous);

      schemas.add(querySchema);
      previous.add(pathParser.parseTablePath(rule.getSource()).getFullPath());
      mapping.addTables(querySchema);

      for (int k = 0; k < columnRules.size(); k++) {
        MappingRule column = columnRules.get(k);
        if (seenProperties.contains(column.getTarget())) {
          continue;
        }

        if (column.getTarget().equals("$")) {
          SqlQueryColumn column1 = querySchema.getColumns().get(k);

          if (column1.hasOperation(SqlQueryColumn.Operation.CONNECTOR)) {
            List<FeatureSchema> connectedSchemas =
                includeSchema
                    ? getConnectedSchemas(schema, column1.getPathSegment(), "", false)
                    : List.of();

            for (FeatureSchema p : connectedSchemas) {
              mapping.putValueTables(p.getFullPathAsString(), querySchema);
              mapping.putValueColumnIndexes(p.getFullPathAsString(), k);
              mapping.putValueSchemas(p.getFullPathAsString(), p);
            }
          }
        } else {
          mapping.putValueTables(column.getTarget(), querySchema);
          mapping.putValueColumnIndexes(column.getTarget(), k);
          seenProperties.add(column.getTarget());

          if (includeSchema && !MappingRulesDeriver.doIgnore(column.getTarget())) {
            mapping.putValueSchemas(
                column.getTarget(),
                schema.getAllNestedProperties().stream()
                    .filter(property -> matches(column, property))
                    .findFirst()
                    .orElseThrow(
                        () -> {
                          List<FeatureSchema> allNestedProperties = schema.getAllNestedProperties();
                          List<FeatureSchema> allNestedConcatProperties =
                              schema.getAllNestedConcatProperties();
                          return new IllegalStateException(
                              "Schema not found for property: " + column.getIdentifier());
                        }));
          }
        }
      }

      i = j;
    }

    return mapping.build();
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

              if (p.isValue()) {
                if (!inArray && pathInConnector.isEmpty()) {
                  return Stream.of(p);
                }

                return Stream.of(
                    new ImmutableFeatureSchema.Builder()
                        .from(p)
                        .putAdditionalInfo(IN_CONNECTED_ARRAY, String.valueOf(inArray))
                        .putAdditionalInfo(PATH_IN_CONNECTOR, newPathInConnector)
                        .build());
              }

              return getConnectedSchemas(p, null, newPathInConnector, inArray || p.isArray())
                  .stream();
            })
        .toList();
  }

  private SqlQuerySchema derive(
      MappingRule table, List<MappingRule> columns, List<List<String>> previous) {
    SqlPath sqlPath = pathParser.parseFullTablePath(table.getSource());

    ImmutableSqlQuerySchema querySchema =
        new Builder()
            .name(sqlPath.getName())
            .pathSegment(sqlPath.asPath())
            .sortKey(sqlPath.getSortKey())
            .filter(sqlPath.getFilter().map(expr -> (Operation<?>) expr))
            .columns(columns.stream().map(this::getColumn).toList())
            .relations(getJoins(sqlPath, previous))
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
                .build());
      }
    }

    return joins;
  }

  private SqlQueryColumn getColumn(MappingRule column) {
    SqlPath sqlPath = pathParser.parseColumnPath(column.getSource());

    return new ImmutableSqlQueryColumn.Builder()
        .name(sqlPath.getName())
        .pathSegment(sqlPath.asPath())
        .type(column.getType())
        .role(column.getRole())
        .operations(getColumnOperations(column, sqlPath))
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

  // TODO: other ops
  private Map<SqlQueryColumn.Operation, String[]> getColumnOperations(
      MappingRule column, SqlPath sqlPath) {

    if (sqlPath.getConstantValue().isPresent()) {
      return Map.of(
          SqlQueryColumn.Operation.CONSTANT, new String[] {sqlPath.getConstantValue().get()});
    }

    if (sqlPath.isConnected()) {
      return Map.of(
          SqlQueryColumn.Operation.CONNECTOR, new String[] {sqlPath.getConnector().orElse("")});
    }

    if (column.getType() == Type.GEOMETRY) {
      SqlQueryColumn.Operation op =
          queryGeneration.getGeometryAsWkb()
              ? SqlQueryColumn.Operation.WKB
              : SqlQueryColumn.Operation.WKT;

      return Map.of(op, new String[] {});
    }

    // TODO: format from mapping
    if (column.getType() == Type.DATETIME) {
      return Map.of(SqlQueryColumn.Operation.DATETIME, new String[] {});
    }

    // TODO: format from mapping
    if (column.getType() == Type.DATE) {
      return Map.of(SqlQueryColumn.Operation.DATE, new String[] {});
    }

    return Map.of();
  }
}
