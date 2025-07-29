/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation;
import java.util.Map;
import java.util.Set;

public interface SqlQueryColumnOperations {

  static String getQualifiedColumn(String tableAlias, SqlQueryColumn column) {
    String columnName = column.getName();

    return columnName.contains("(")
        ? columnName.replaceAll("((?:\\w+\\()+)(\\w+)((?:\\))+)", "$1" + tableAlias + ".$2$3 AS $2")
        : String.format("%s.%s", tableAlias, columnName);
  }

  static String getQualifiedColumnResolved(
      String tableAlias, SqlQueryColumn column, SqlDialect sqlDialect) {
    return getQualifiedColumnResolved(tableAlias, column, sqlDialect, Set.of(), false);
  }

  static String getQualifiedColumnResolved(
      String tableAlias,
      SqlQueryColumn column,
      SqlDialect sqlDialect,
      Set<Operation> excludeOperations,
      boolean forceLinearizeCurves) {
    String name = getQualifiedColumn(tableAlias, column);

    Map<Operation, String[]> ops = column.getOperations();

    if (ops.containsKey(Operation.CONSTANT) && !excludeOperations.contains(Operation.CONSTANT)) {
      return "'%s' AS %s"
          .formatted(column.getOperationParameter(Operation.CONSTANT, ""), column.getName());
    }
    if (ops.containsKey(Operation.EXPRESSION)
        && !excludeOperations.contains(Operation.EXPRESSION)) {
      final int[] i = {0};
      return sqlDialect.applyToExpression(
          tableAlias,
          column.getName(),
          column.getOperationParameters(Operation.EXPRESSION).stream()
              .map(param -> Map.entry("" + i[0]++, param))
              .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)),
          ops.containsKey(Operation.WKT));
    }
    if (ops.containsKey(Operation.WKT) && !excludeOperations.contains(Operation.WKT)) {
      boolean isForcePolygonCCW = ops.containsKey(Operation.FORCE_POLYGON_CCW);
      boolean shouldLinearizeCurves =
          ops.containsKey(Operation.LINEARIZE_CURVES) || forceLinearizeCurves;
      return sqlDialect.applyToWkt(name, isForcePolygonCCW, shouldLinearizeCurves);
    }
    if (ops.containsKey(Operation.WKB) && !excludeOperations.contains(Operation.WKB)) {
      boolean isForcePolygonCCW = ops.containsKey(Operation.FORCE_POLYGON_CCW);
      boolean shouldLinearizeCurves =
          ops.containsKey(Operation.LINEARIZE_CURVES) || forceLinearizeCurves;
      return sqlDialect.applyToWkb(name, isForcePolygonCCW, shouldLinearizeCurves);
    }
    if (ops.containsKey(Operation.DATE) && !excludeOperations.contains(Operation.DATE)) {
      return sqlDialect.applyToDate(name, column.getOperationParameter(Operation.DATE));
    }
    if (ops.containsKey(Operation.DATETIME) && !excludeOperations.contains(Operation.DATETIME)) {
      return sqlDialect.applyToDatetime(name, column.getOperationParameter(Operation.DATETIME));
    }
    return name;
  }

  static SqlQueryColumn dateToDatetime(SqlQueryColumn column) {
    if (!column.getOperations().containsKey(Operation.DATE)) {
      return column;
    }
    Builder<Operation, String[]> ops =
        ImmutableMap.<Operation, String[]>builder()
            .put(
                Operation.DATETIME,
                column.getOperationParameters(Operation.DATE).toArray(new String[0]));

    for (Map.Entry<Operation, String[]> entry : column.getOperations().entrySet()) {
      if (entry.getKey() != Operation.DATE) {
        ops.put(entry);
      }
    }

    return new ImmutableSqlQueryColumn.Builder().from(column).operations(ops.build()).build();
  }
}
