/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SortKey;
import de.ii.xtraplatform.features.domain.SortKey.Direction;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: extensive unit tests for compareTo
class SqlRowVals implements SqlRow {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlRowVals.class);

  private final List<Comparable<?>> ids;
  private final List<Comparable<?>> sortKeys;
  private List<String> sortKeyNames;
  private List<SortKey.Direction> sortKeyDirections;
  private final List<Object> values;
  private int priority;
  private SqlQuerySchema tableSchema;
  private Optional<String> type;
  @Nullable private final Collator collator;

  SqlRowVals() {
    this(null);
  }

  SqlRowVals(@Nullable Collator collator) {
    this.ids = new ArrayList<>(32);
    this.sortKeys = new ArrayList<>(32);
    this.sortKeyNames = new ArrayList<>(32);
    this.values = new ArrayList<>(128);
    this.collator = collator;
  }

  @Override
  public List<Object> getValues() {
    return values;
  }

  @Override
  public String getName() {
    if (Objects.nonNull(tableSchema)) {
      return tableSchema.getName();
    }
    return null;
  }

  @Override
  public List<String> getPath() {
    if (Objects.nonNull(tableSchema)) {
      return tableSchema.getFullPath();
    }
    return ImmutableList.of();
  }

  @Override
  public int getPriority() {
    return priority;
  }

  @Override
  public List<Comparable<?>> getIds() {
    return ids;
  }

  @Override
  public List<Comparable<?>> getSortKeys() {
    return sortKeys;
  }

  @Override
  public List<String> getSortKeyNames() {
    return sortKeyNames;
  }

  @Override
  public Optional<String> getType() {
    return type;
  }

  @Override
  public List<List<String>> getColumnPaths() {
    if (Objects.nonNull(tableSchema)) {
      return tableSchema.getColumnPaths();
    }
    return ImmutableList.of();
  }

  private boolean hasColumnSchema(int i) {
    return Objects.nonNull(tableSchema) && tableSchema.getColumns().size() > i;
  }

  @Override
  public boolean isSpatialColumn(int i) {
    return hasColumnSchema(i) && tableSchema.getColumns().get(i).getType() == Type.GEOMETRY;
  }

  @Override
  public boolean isTemporalColumn(int i) {
    return hasColumnSchema(i)
        && (tableSchema.getColumns().get(i).getType() == Type.DATE
            || tableSchema.getColumns().get(i).getType() == Type.DATETIME);
  }

  @Override
  public boolean isSubDecoderColumn(int i) {
    return hasColumnSchema(i)
        && tableSchema.getColumns().get(i).getOperations().containsKey(Operation.CONNECTOR);
  }

  @Override
  public String getSubDecoder(int i) {
    return isSubDecoderColumn(i)
        ? Arrays.stream(tableSchema.getColumns().get(i).getOperations().get(Operation.CONNECTOR))
            .findFirst()
            .orElse("")
        : "";
  }

  // TODO: use result.nextObject when column type info is supported
  SqlRow read(ResultSet result, SqlQueryOptions queryOptions) {
    this.priority = queryOptions.getContainerPriority();
    List<Class<?>> columnTypes;
    int cursor = 1;

    // TODO: only returnables
    if (queryOptions.getTableSchema().isPresent()) {
      this.tableSchema = queryOptions.getTableSchema().get();
      this.type = queryOptions.getType();
      this.sortKeyNames = queryOptions.getSortKeys();
      this.sortKeyDirections = queryOptions.getSortDirections();
      columnTypes = queryOptions.getColumnTypes();

      for (int i = 0; i < sortKeyNames.size(); i++) {
        try {
          Object id = result.getObject(cursor++);
          if (Objects.isNull(id)) {
            sortKeys.add(null);
            if (i >= queryOptions.getCustomSortKeys().size()) {
              throw new IllegalStateException(
                  String.format(
                      "Primary sort key %s of table %s may not be null.",
                      sortKeyNames.get(i), tableSchema.getName()));
            }
          } else if (id instanceof Comparable<?>) {
            sortKeys.add((Comparable<?>) id);
            if (i >= queryOptions.getCustomSortKeys().size()) {
              ids.add((Comparable<?>) id);
            }
          } else {
            LOGGER.error(
                "Sort key '{}' has invalid type '{}'.", sortKeyNames.get(i), id.getClass());
          }
        } catch (Throwable e) {
          break;
        }
      }

    } else {
      columnTypes = queryOptions.getColumnTypes();
    }

    for (int i = 0; i < columnTypes.size(); i++) {
      try {
        values.add(getValue(result, cursor++, columnTypes.get(i)));
      } catch (Throwable e) {
        break;
      }
    }

    return this;
  }

  private Object getValue(ResultSet result, int cursor, Class<?> type) throws SQLException {
    if (type == BigDecimal.class) return result.getBigDecimal(cursor);
    if (type == Blob.class) return result.getBlob(cursor);
    if (type == Byte.class) return result.getByte(cursor);
    if (type == byte[].class) return result.getBytes(cursor);
    if (type == Clob.class) return result.getClob(cursor);
    if (type == Date.class) return result.getDate(cursor);
    if (type == Double.class) return result.getDouble(cursor);
    if (type == Float.class) return result.getFloat(cursor);
    if (type == Integer.class) return result.getInt(cursor);
    if (type == Long.class) return result.getLong(cursor);
    if (type == Object.class) return result.getObject(cursor);
    if (type == Short.class) return result.getShort(cursor);
    if (type == String.class) return result.getString(cursor);
    if (type == Time.class) return result.getTime(cursor);
    if (type == Timestamp.class) return result.getTimestamp(cursor);

    return result.getString(cursor);
  }

  void clear() {
    this.values.clear();
    this.ids.clear();
    this.sortKeyNames = null;
    this.priority = 0;
    this.tableSchema = null;
  }

  // TODO: move comparison to SqlRow
  @Override
  public int compareTo(SqlRow otherSqlRow) {
    if (Objects.isNull(tableSchema)) {
      return -1;
    }

    int commonSortKeys = getNumberOfCommonElements(sortKeyNames, otherSqlRow.getSortKeyNames());
    int resultSortKeys =
        compareSortKeys(
            getSortKeys(), otherSqlRow.getSortKeys(), commonSortKeys, sortKeyDirections, collator);
    int result = resultSortKeys == 0 ? priority - otherSqlRow.getPriority() : resultSortKeys;

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Compare: {}[{}{}] <=> {}[{}{}] -> {}({})",
          getName(),
          sortKeyNames,
          sortKeys,
          otherSqlRow.getName(),
          otherSqlRow.getSortKeyNames(),
          otherSqlRow.getSortKeys(),
          result,
          resultSortKeys);
    }

    return result;
  }

  private static int getNumberOfCommonElements(List<String> list1, List<String> list2) {
    int size = 0;
    for (int i = 0; i < list1.size() && i < list2.size(); i++) {
      if (!Objects.equals(list1.get(i), list2.get(i))) {
        break;
      }
      size = i + 1;
    }
    return size;
  }

  private static int compareSortKeys(
      List<Comparable<?>> ids1,
      List<Comparable<?>> ids2,
      int numberOfIds,
      List<Direction> idColumnDirections,
      Collator collator) {
    for (int i = 0; i < numberOfIds; i++) {
      int result = 0;
      Comparable<?> id1 = ids1.get(i);
      Comparable<?> id2 = ids2.get(i);
      int direction = idColumnDirections.get(i) == Direction.DESCENDING ? -1 : 1;

      if (Objects.isNull(id1) && Objects.isNull(id2)) {
        result = 0;
      } else if (Objects.isNull(id1)) {
        result = -1;
      } else if (Objects.isNull(id2)) {
        result = 1;
      } else if (id1 instanceof Integer) {
        result = ((Integer) id1).compareTo((Integer) id2);
      } else if (id1 instanceof Long) {
        result = ((Long) id1).compareTo((Long) id2);
      } else if (id1 instanceof Short) {
        result = ((Short) id1).compareTo((Short) id2);
      } else if (id1 instanceof Double) {
        result = ((Double) id1).compareTo((Double) id2);
      } else if (id1 instanceof BigDecimal) {
        result = ((BigDecimal) id1).compareTo((BigDecimal) id2);
      } else if (id1 instanceof Float) {
        result = ((Float) id1).compareTo((Float) id2);
      } else if (id1 instanceof Date) {
        result = ((Date) id1).compareTo((Date) id2);
      } else if (id1 instanceof Timestamp) {
        result = ((Timestamp) id1).compareTo((Timestamp) id2);
      } else if (Objects.nonNull(collator)) {
        result = collator.compare((String) id1, (String) id2);
      } else {
        result = ((String) id1).compareTo((String) id2);
      }
      if (result != 0) {
        return result * direction;
      }
    }
    return 0;
  }

  @Override
  public String toString() {
    return "SqlRowSlick{"
        + "ids='"
        + ids
        + '\''
        + ", name='"
        + getName()
        + '\''
        + ", values="
        + values
        + ", priority="
        + priority
        + ", path="
        + getPath()
        + '}';
  }
}
