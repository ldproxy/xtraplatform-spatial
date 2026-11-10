/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.cql.domain.And;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class AggregateStatsQueryGenerator {

  private static final String FORMAT_TABLE_ALIAS = "%s %s";
  private static final String FORMAT_WHERE = " WHERE %s";

  /**
   * The temporal extent queries return no row if there is nothing to aggregate, so that NULL in the
   * result always stands for an unbounded start or end.
   */
  private static final String FORMAT_TEMPORAL_EXTENT =
      "SELECT T.extent_start, T.extent_end FROM (%s) T WHERE T.row_count > 0";

  private final SqlDialect sqlDialect;
  private final FilterEncoderSql filterEncoder;

  public AggregateStatsQueryGenerator(SqlDialect sqlDialect, FilterEncoderSql filterEncoder) {
    this.sqlDialect = sqlDialect;
    this.filterEncoder = filterEncoder;
  }

  public String getCountQuery(SqlQueryMapping mapping) {
    return getCountQuery(mapping, List.of());
  }

  /**
   * Counts the features that match the filter of the main table and the additional filters, e.g.
   * the restriction of all queries to a fixed extent.
   */
  public String getCountQuery(SqlQueryMapping mapping, List<Cql2Expression> additionalFilters) {
    SqlQuerySchema sourceSchema = mapping.getMainTable();

    List<String> aliases = AliasGenerator.getAliases(sourceSchema);

    String mainTable = String.format(FORMAT_TABLE_ALIAS, sourceSchema.getName(), aliases.get(0));

    List<Cql2Expression> filters = new ArrayList<>();
    sourceSchema.getFilter().ifPresent(filters::add);
    filters.addAll(additionalFilters);

    Optional<String> filter =
        filters.isEmpty()
            ? Optional.empty()
            : Optional.of(
                filterEncoder.encode(
                    filters.size() == 1 ? filters.get(0) : And.of(filters), mapping));
    String where = filter.isPresent() ? String.format(FORMAT_WHERE, filter.get()) : "";

    return String.format("SELECT COUNT(*) FROM %s%s", mainTable, where);
  }

  public String getSpatialExtentQuery(
      SqlQueryMapping mapping, SqlQuerySchema spatial, SqlQueryColumn spatialColumn, boolean is3d) {
    SqlQuerySchema mainSchema = mapping.getMainTable();

    List<String> aliases = AliasGenerator.getAliases(spatial);
    String spatialAlias = aliases.get(aliases.size() - 1);

    String mainTable = String.format(FORMAT_TABLE_ALIAS, mainSchema.getName(), aliases.get(0));

    String column =
        SqlQueryColumnOperations.getQualifiedColumnResolved(
            spatialAlias, spatialColumn, sqlDialect, Set.of(Operation.WKB, Operation.WKT), false);
    if (column.contains(" AS ") && !column.endsWith(")")) {
      column = column.substring(0, column.indexOf(" AS "));
    }

    String columnExtent = sqlDialect.applyToExtent(column, is3d);

    String join = JoinGenerator.getJoins(spatial, aliases, filterEncoder);

    Optional<String> filter = getFilter(mapping, mainSchema);
    String where = filter.isPresent() ? String.format(FORMAT_WHERE, filter.get()) : "";

    return String.format(
        "SELECT %s FROM %s%s%s%s", columnExtent, mainTable, join.isEmpty() ? "" : " ", join, where);
  }

  public String getTemporalExtentQuery(
      SqlQueryMapping mapping, SqlQuerySchema instant, SqlQueryColumn instantColumn) {
    SqlQuerySchema mainSchema = mapping.getMainTable();

    List<String> aliases = AliasGenerator.getAliases(instant);
    String temporalAlias = aliases.get(aliases.size() - 1);

    String mainTable = String.format(FORMAT_TABLE_ALIAS, mainSchema.getName(), aliases.get(0));

    SqlQueryColumn instantColumnDatetime = SqlQueryColumnOperations.dateToDatetime(instantColumn);

    String column =
        SqlQueryColumnOperations.getQualifiedColumnResolved(
            temporalAlias, instantColumnDatetime, sqlDialect);

    String join = JoinGenerator.getJoins(instant, aliases, filterEncoder);

    Optional<String> filter = getFilter(mapping, mainSchema);
    String where = filter.isPresent() ? String.format(FORMAT_WHERE, filter.get()) : "";

    return String.format(
        FORMAT_TEMPORAL_EXTENT,
        String.format(
            "SELECT MIN(%s) AS extent_start, MAX(%s) AS extent_end, COUNT(%s) AS row_count FROM %s%s%s%s",
            column, column, column, mainTable, join.isEmpty() ? "" : " ", join, where));
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

      String mainTable = String.format(FORMAT_TABLE_ALIAS, mainSchema.getName(), aliases.get(0));

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
      String where = filter.isPresent() ? String.format(FORMAT_WHERE, filter.get()) : "";

      return String.format(
          FORMAT_TEMPORAL_EXTENT,
          String.format(
              "SELECT %s AS extent_start, %s AS extent_end, COUNT(*) AS row_count FROM %s%s%s%s",
              minOrUnbounded(columnStart),
              maxOrUnbounded(columnEnd),
              mainTable,
              join.isEmpty() ? "" : " ",
              join,
              where));

    } else {
      List<String> startAliases = AliasGenerator.getAliases(intervalStart);
      String startAlias = startAliases.get(startAliases.size() - 1);
      List<String> endAliases = AliasGenerator.getAliases(intervalEnd);
      String endAlias = endAliases.get(endAliases.size() - 1);

      String mainTable =
          String.format(FORMAT_TABLE_ALIAS, mainSchema.getName(), startAliases.get(0));

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
      String where = filter.isPresent() ? String.format(FORMAT_WHERE, filter.get()) : "";

      return String.format(
          "SELECT S.extent_start, E.extent_end FROM (SELECT %s AS extent_start, COUNT(*) AS"
              + " row_count FROM %s%s) S, (SELECT %s AS extent_end FROM %s%s) E WHERE S.row_count >"
              + " 0",
          minOrUnbounded(columnStart),
          startTableWithJoins,
          where,
          maxOrUnbounded(columnEnd),
          endTableWithJoins,
          where);
    }
  }

  /** The earliest start, or NULL (unbounded) if any start is NULL. */
  private static String minOrUnbounded(String column) {
    return String.format("CASE WHEN COUNT(*) > COUNT(%1$s) THEN NULL ELSE MIN(%1$s) END", column);
  }

  /** The latest end, or NULL (unbounded) if any end is NULL. */
  private static String maxOrUnbounded(String column) {
    return String.format("CASE WHEN COUNT(*) > COUNT(%1$s) THEN NULL ELSE MAX(%1$s) END", column);
  }

  private Optional<String> getFilter(SqlQueryMapping mapping, SqlQuerySchema schemaSql) {
    return schemaSql.getFilter().map(cql -> filterEncoder.encode(cql, mapping));
  }
}
