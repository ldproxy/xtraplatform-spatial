/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.sql.domain.SchemaSql;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateStatsQueryGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateStatsQueryGenerator.class);

  private final SqlDialect sqlDialect;
  private final FilterEncoderSql filterEncoder;

  public AggregateStatsQueryGenerator(SqlDialect sqlDialect, FilterEncoderSql filterEncoder) {
    this.sqlDialect = sqlDialect;
    this.filterEncoder = filterEncoder;
  }

  public String getCountQuery(SqlQueryMapping mapping) {
    SqlQuerySchema sourceSchema = mapping.getMainTable();

    List<String> aliases = AliasGenerator.getAliases(sourceSchema);

    String mainTable = String.format("%s %s", sourceSchema.getName(), aliases.get(0));

    Optional<String> filter = getFilter(mapping, sourceSchema);
    String where = filter.isPresent() ? String.format(" WHERE %s", filter.get()) : "";

    return String.format("SELECT COUNT(*) FROM %s%s", mainTable, where);
  }

  public String getSpatialExtentQuery(
      SqlQueryMapping mapping, SqlQuerySchema spatial, SqlQueryColumn spatialColumn, boolean is3d) {
    SqlQuerySchema mainSchema = mapping.getMainTable();

    List<String> aliases = AliasGenerator.getAliases(spatial);
    String spatialAlias = aliases.get(aliases.size() - 1);

    String mainTable = String.format("%s %s", mainSchema.getName(), aliases.get(0));

    String column =
        SqlQueryColumnOperations.getQualifiedColumnResolved(
            spatialAlias, spatialColumn, sqlDialect, Set.of(Operation.WKB, Operation.WKT));
    if (column.contains(" AS ") && !column.endsWith(")")) {
      column = column.substring(0, column.indexOf(" AS "));
    }

    String columnExtent = sqlDialect.applyToExtent(column, is3d);

    String join = JoinGenerator.getJoins(spatial, aliases, filterEncoder);

    Optional<String> filter = getFilter(mapping, mainSchema);
    String where = filter.isPresent() ? String.format(" WHERE %s", filter.get()) : "";

    return String.format(
        "SELECT %s FROM %s%s%s%s", columnExtent, mainTable, join.isEmpty() ? "" : " ", join, where);
  }

  public String getTemporalExtentQuery(
      SqlQueryMapping mapping, SqlQuerySchema instant, SqlQueryColumn instantColumn) {
    SqlQuerySchema mainSchema = mapping.getMainTable();

    List<String> aliases = AliasGenerator.getAliases(instant);
    String temporalAlias = aliases.get(aliases.size() - 1);

    String mainTable = String.format("%s %s", mainSchema.getName(), aliases.get(0));

    SqlQueryColumn instantColumnDatetime = SqlQueryColumnOperations.dateToDatetime(instantColumn);

    String column =
        SqlQueryColumnOperations.getQualifiedColumnResolved(
            temporalAlias, instantColumnDatetime, sqlDialect);

    String join = JoinGenerator.getJoins(instant, aliases, filterEncoder);

    Optional<String> filter = getFilter(mapping, mainSchema);
    String where = filter.isPresent() ? String.format(" WHERE %s", filter.get()) : "";

    return String.format(
        "SELECT MIN(%s), MAX(%s) FROM %s%s%s%s",
        column, column, mainTable, join.isEmpty() ? "" : " ", join, where);
  }

  public String getTemporalExtentQuery(
      SqlQueryMapping mapping,
      SqlQuerySchema intervalStart,
      SqlQueryColumn intervalStartColumn,
      SqlQuerySchema intervalEnd,
      SqlQueryColumn intervalEndColumn) {
    SqlQuerySchema mainSchema = mapping.getMainTable();

    if (Objects.equals(intervalStart, intervalEnd)) {
      List<String> aliases = AliasGenerator.getAliases(intervalStart);
      String temporalAlias = aliases.get(aliases.size() - 1);

      String mainTable = String.format("%s %s", mainSchema.getName(), aliases.get(0));

      SqlQueryColumn intervalStartColumnColumnDatetime =
          SqlQueryColumnOperations.dateToDatetime(intervalStartColumn);
      SqlQueryColumn intervalEndColumnDatetime =
          SqlQueryColumnOperations.dateToDatetime(intervalEndColumn);

      String columnStart =
          SqlQueryColumnOperations.getQualifiedColumnResolved(
              temporalAlias, intervalStartColumnColumnDatetime, sqlDialect);

      String columnEnd =
          SqlQueryColumnOperations.getQualifiedColumnResolved(
              temporalAlias, intervalEndColumnDatetime, sqlDialect);

      String join = JoinGenerator.getJoins(intervalStart, aliases, filterEncoder);

      Optional<String> filter = getFilter(mapping, mainSchema);
      String where = filter.isPresent() ? String.format(" WHERE %s", filter.get()) : "";

      return String.format(
          "SELECT MIN(%s), MAX(%s) FROM %s%s%s%s",
          columnStart, columnEnd, mainTable, join.isEmpty() ? "" : " ", join, where);

    } else {
      List<String> startAliases = AliasGenerator.getAliases(intervalStart);
      String startAlias = startAliases.get(startAliases.size() - 1);
      List<String> endAliases = AliasGenerator.getAliases(intervalEnd);
      String endAlias = endAliases.get(endAliases.size() - 1);

      String mainTable = String.format("%s %s", mainSchema.getName(), startAliases.get(0));

      String columnStart =
          SqlQueryColumnOperations.getQualifiedColumnResolved(
              startAlias, intervalStartColumn, sqlDialect);

      String columnEnd =
          SqlQueryColumnOperations.getQualifiedColumnResolved(
              endAlias, intervalEndColumn, sqlDialect);

      String startJoin = JoinGenerator.getJoins(intervalStart, startAliases, filterEncoder);
      String startTableWithJoins =
          String.format("%s%s%s", mainTable, startJoin.isEmpty() ? "" : " ", startJoin);
      String endJoin = JoinGenerator.getJoins(intervalEnd, endAliases, filterEncoder);
      String endTableWithJoins =
          String.format("%s%s%s", mainTable, endJoin.isEmpty() ? "" : " ", endJoin);

      Optional<String> filter = getFilter(mapping, mainSchema);
      String where = filter.isPresent() ? String.format(" WHERE %s", filter.get()) : "";

      return String.format(
          "SELECT * FROM (SELECT MIN(%s) FROM %s%s) AS A, (SELECT MAX(%s) from %s%s) AS B;",
          columnStart, startTableWithJoins, where, columnEnd, endTableWithJoins, where);
    }
  }

  private Optional<String> getFilter(SchemaSql schemaSql) {
    return schemaSql.getFilter().map(cql -> filterEncoder.encode(cql, schemaSql));
  }

  private Optional<String> getFilter(SqlQueryMapping mapping, SqlQuerySchema schemaSql) {
    return schemaSql.getFilter().map(cql -> filterEncoder.encode(cql, mapping));
  }

  private String getQualifiedColumn(String table, String column) {
    if (column.startsWith("[EXPRESSION]{sql=")) {
      return column.substring(17, column.length() - 1).replaceAll("\\$T\\$", table);
    }
    return column.contains("(")
        ? column.replaceAll("((?:\\w+\\()+)(\\w+)((?:\\))+)", "$1" + table + ".$2$3 AS $2")
        : String.format("%s.%s", table, column);
  }
}
