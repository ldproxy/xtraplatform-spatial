/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.cql.domain.Operation;
import de.ii.xtraplatform.features.domain.MappingRule;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryJoin;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQuerySchema;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQuerySchema.Builder;
import de.ii.xtraplatform.features.sql.domain.SqlPath;
import de.ii.xtraplatform.features.sql.domain.SqlPath.JoinType;
import de.ii.xtraplatform.features.sql.domain.SqlPathParser;
import de.ii.xtraplatform.features.sql.domain.SqlQueryJoin;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class SqlMappingDeriver {

  private final SqlPathParser pathParser;

  public SqlMappingDeriver(SqlPathParser pathParser) {
    this.pathParser = pathParser;
  }

  public SqlQueryMapping derive(List<MappingRule> mappingRules) {
    List<SqlQuerySchema> schemas = new ArrayList<>();
    List<List<String>> previous = new ArrayList<>();
    ImmutableSqlQueryMapping.Builder mapping = new ImmutableSqlQueryMapping.Builder();
    List<String> seenProperties = new ArrayList<>();
    int i = 0;

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
        String propertyAccessor = getPropertyAccessor(column.getTarget());
        if (seenProperties.contains(propertyAccessor)) {
          continue;
        }
        mapping.putValueTables(propertyAccessor, querySchema);
        mapping.putValueColumnIndexes(propertyAccessor, k);
        seenProperties.add(propertyAccessor);
      }

      i = j;
    }

    return mapping.build();
  }

  private SqlQuerySchema derive(
      MappingRule table, List<MappingRule> columns, List<List<String>> previous) {
    SqlPath sqlPath = pathParser.parseFullTablePath(table.getSource());

    ImmutableSqlQuerySchema querySchema =
        new Builder()
            .name(sqlPath.getName())
            .sortKey(sqlPath.getSortKey())
            .filter(sqlPath.getFilter().map(expr -> (Operation<?>) expr))
            .columns(columns.stream().map(this::getColumnName).toList())
            .columnOperations(asMap(columns.stream().map(this::getColumnOperation).toList()))
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

  private String getColumnName(MappingRule column) {
    SqlPath sqlPath = pathParser.parseColumnPath(column.getSource());
    return sqlPath.getName();
    // return column.getPath().substring(column.getPath().lastIndexOf("/") + 1);
  }

  private Optional<Map<SqlQuerySchema.Operation, String>> getColumnOperation(MappingRule column) {
    SqlPath sqlPath = pathParser.parseColumnPath(column.getSource());

    if (sqlPath.getConstantValue().isPresent()) {
      return Optional.of(
          Map.of(SqlQuerySchema.Operation.CONSTANT, sqlPath.getConstantValue().get()));
    }
    return Optional.empty();
  }

  private static Map<Integer, Map<SqlQuerySchema.Operation, String>> asMap(
      List<Optional<Map<SqlQuerySchema.Operation, String>>> list) {
    Map<Integer, Map<SqlQuerySchema.Operation, String>> map = new LinkedHashMap<>();

    for (int i = 0; i < list.size(); i++) {
      Optional<Map<SqlQuerySchema.Operation, String>> optional = list.get(i);
      if (optional.isPresent()) {
        map.put(i, optional.get());
      }
    }

    return map;
  }

  private static String getPropertyAccessor(String propertyPath) {
    int start = propertyPath.indexOf("/", 1) + 1;
    return propertyPath.substring(start).replaceAll("/", ".");
  }
}
