/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.domain.ValueTypeMapping;
import java.sql.JDBCType;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import schemacrawler.schema.Column;
import schemacrawler.schema.Index;
import schemacrawler.schema.Table;
import schemacrawler.schema.View;

class SchemaInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaInfo.class);

  private static final String VIEW_COLUMN_NOT_ANALYZABLE =
      "Cannot analyze constraints and indices for column '{}' in view '{}', the column might be composed of multiple actual columns or the view might be composed of other views.";

  private final Collection<Table> tables;

  SchemaInfo(Collection<Table> tables) {
    this.tables = tables;
  }

  public boolean tableExists(String name) {
    return tables.stream().anyMatch(t -> t.getName().equals(name));
  }

  public boolean columnExists(String name, String table) {
    return tables.stream()
        .filter(t -> t.getName().equals(table))
        .flatMap(t -> t.getColumns().stream())
        .anyMatch(c -> c.getName().equals(name));
  }

  // TODO: unique may be either column constraint, table constraint or index plus primary key
  // TODO: NOT NULL constraints
  public boolean isColumnUnique(String columnName, String tableName) {
    return isColumnUnique(columnName, tableName, true);
  }

  public boolean isColumnUnique(String columnName, String tableName, boolean warn) {
    Optional<Column> optionalColumn = getColumn(tableName, columnName, true, warn);

    if (optionalColumn.isPresent()) {
      Column column = optionalColumn.get();
      Table table = column.getParent();

      boolean isPrimaryKey =
          table.hasPrimaryKey()
              && table.getPrimaryKey().getConstrainedColumns().size() == 1
              && Objects.equals(
                  table.getPrimaryKey().getConstrainedColumns().get(0).getFullName(),
                  column.getFullName());

      boolean isUniqueIndex =
          table.getIndexes().stream()
              .filter(Index::isUnique)
              .anyMatch(
                  index ->
                      index.getColumns().size() == 1
                          && Objects.equals(index.getColumns().get(0), column));

      return isPrimaryKey || isUniqueIndex;
    }

    return false;
  }

  public boolean isColumnReadOnly(String columnName, String tableName) {
    return getColumn(tableName, columnName, true, false)
        .map(column -> column.isAutoIncremented() || column.isGenerated())
        .orElse(false);
  }

  public boolean isColumnSpatial(String table, String name) {
    return getColumn(table, name)
        .filter(
            c ->
                ValueTypeMapping.matches(
                    c.getColumnDataType().getJavaSqlType(),
                    c.getColumnDataType().getName(),
                    Type.GEOMETRY))
        .isPresent();
  }

  public boolean isColumnTemporal(String table, String name) {
    return getColumn(table, name)
        .filter(
            c ->
                ValueTypeMapping.matches(
                    c.getColumnDataType().getJavaSqlType(),
                    c.getColumnDataType().getDatabaseSpecificTypeName(),
                    Type.DATETIME))
        .isPresent();
  }

  /**
   * Whether the column has a time zone, e.g. PostgreSQL {@code timestamptz}. Drivers differ in the
   * JDBC type they report for such columns, so the database-specific type name is checked, too.
   */
  public boolean isColumnTemporalWithTimeZone(String table, String name) {
    return getColumn(table, name)
        .map(Column::getColumnDataType)
        .filter(
            t ->
                Objects.equals(t.getJavaSqlType(), JDBCType.TIMESTAMP_WITH_TIMEZONE)
                    || Objects.equals(t.getJavaSqlType(), JDBCType.TIME_WITH_TIMEZONE)
                    || hasTimeZoneInName(t.getDatabaseSpecificTypeName())
                    || hasTimeZoneInName(t.getName()))
        .isPresent();
  }

  private static boolean hasTimeZoneInName(String typeName) {
    if (Objects.isNull(typeName)) {
      return false;
    }
    // "timestamptz"/"timetz" (PostgreSQL), "TIMESTAMP WITH TIME ZONE" and Oracle's
    // "TIMESTAMP WITH LOCAL TIME ZONE"; "timestamp without time zone" must not match
    String name = typeName.toLowerCase(Locale.ROOT);
    return name.endsWith("tz")
        || name.contains("with time zone")
        || name.contains("with local time zone");
  }

  public Optional<Column> getColumn(String tableName, String columnName) {
    return getColumn(tableName, columnName, false, false);
  }

  public Optional<Column> getColumn(
      String tableName, String columnName, boolean resolveViews, boolean warn) {
    Optional<Column> optionalColumn =
        tables.stream()
            .filter(t -> t.getName().equals(tableName))
            .flatMap(t -> t.getColumns().stream())
            .filter(c -> c.getName().equals(columnName))
            .findFirst();

    if (resolveViews && optionalColumn.isPresent()) {
      Table table = optionalColumn.get().getParent();

      if (table instanceof View) {
        Optional<Tuple<String, String>> originalColumn =
            ViewInfo.getOriginalTableAndColumn(table.getDefinition(), columnName);

        if (originalColumn.isPresent()) {
          return getColumn(originalColumn.get().first(), originalColumn.get().second());
        }

        if (warn) {
          LOGGER.warn(VIEW_COLUMN_NOT_ANALYZABLE, columnName, tableName);
        }
      }
    }

    return optionalColumn;
  }
}
