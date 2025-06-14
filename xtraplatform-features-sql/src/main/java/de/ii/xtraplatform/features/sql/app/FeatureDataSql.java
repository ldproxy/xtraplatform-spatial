/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.domain.FeatureTransactions;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Modifiable
public interface FeatureDataSql {

  Logger LOGGER = LoggerFactory.getLogger(FeatureDataSql.class);

  SqlQueryMapping getMapping();

  List<Tuple<SqlQuerySchema, ModifiableSqlRowData>> getRows();

  Map<List<String>, List<Integer>> getRowNesting();

  List<Tuple<SqlQuerySchema, AtomicInteger>> getStack();

  default void addRow(SqlQuerySchema table) {
    int newCurrent = -1;

    for (int i = getStack().size() - 1; i >= 0; i--) {
      if (Objects.equals(getStack().get(i).first(), table)) {
        getStack().get(i).second().set(getRows().size());
        newCurrent = i;
        getRows().add(Tuple.of(table, ModifiableSqlRowData.create()));
        break;
      } else if (table
          .getFullPathAsString()
          .startsWith(getStack().get(i).first().getFullPathAsString())) {
        getStack().add(i + 1, Tuple.of(table, new AtomicInteger(getRows().size())));
        newCurrent = i + 1;
        getRows().add(Tuple.of(table, ModifiableSqlRowData.create()));
        break;
      }
    }

    if (newCurrent == -1) {
      getStack().add(Tuple.of(table, new AtomicInteger(getRows().size())));
      newCurrent = getStack().size() - 1;
      getRows().add(Tuple.of(table, ModifiableSqlRowData.create()));
    }

    if (getStack().size() > newCurrent + 1) {
      getStack().subList(newCurrent + 1, getStack().size()).clear();
    }

    getRowNesting().put(table.getFullPath(), getIncrementedRowCounts(table.getFullPath()));
  }

  default void closeRow(SqlQuerySchema table) {
    if (isCurrent(table)) {
      getStack().remove(getStack().size() - 1);
    }
  }

  default void addColumn(SqlQuerySchema table, SqlQueryColumn column, String value) {
    if (isCurrent(table)) {
      getCurrentRow(table).putValues(column.getName(), value);
    }
  }

  default Consumer<String> addLazyColumn(SqlQuerySchema table, SqlQueryColumn column) {
    if (isCurrent(table)) {
      ModifiableSqlRowData currentRow = getCurrentRow(table);
      String columnName = column.getName();

      return value -> {
        if (value != null) {
          currentRow.putValues(columnName, value);
        }
      };
    }
    return value -> {
      // No-op if not current
    };
  }

  default boolean isCurrent(SqlQuerySchema table) {
    return !getStack().isEmpty()
        && Objects.equals(getStack().get(getStack().size() - 1).first(), table);
  }

  default ModifiableSqlRowData getCurrentRow(SqlQuerySchema table) {
    return getRows().get(getStack().get(getStack().size() - 1).second().get()).second();
  }

  // NOTE: json columns work using special handling in the encoder, the patch is applied to original
  default FeatureDataSql patchWith(FeatureDataSql partial) {
    // joins not supported yet
    if (getRows().size() == 1 && partial.getRows().size() == 1) {
      ModifiableFeatureDataSql merged = ModifiableFeatureDataSql.create().from(this);

      for (Tuple<SqlQuerySchema, ModifiableSqlRowData> row : partial.getRows()) {
        Optional<Tuple<SqlQuerySchema, ModifiableSqlRowData>> original =
            merged.getRows().stream()
                .filter(o -> Objects.equals(o.first().getFullPath(), row.first().getFullPath()))
                .findFirst();

        if (original.isEmpty()) {
          continue;
        }

        Map<String, String> originalValues = original.get().second().getValues();
        Map<String, String> patchValues = row.second().getValues();

        for (String prop : patchValues.keySet()) {
          Optional<SqlQueryColumn> column = row.first().findColumn(prop);

          if (column.isEmpty()) {
            continue; // skip if column does not exist in the schema
          }

          String newValue = patchValues.get(prop);

          if (Objects.equals(newValue, FeatureTransactions.PATCH_NULL_VALUE)
              || Objects.equals(newValue, "'" + FeatureTransactions.PATCH_NULL_VALUE + "'")) {
            originalValues.remove(prop);
            continue;
          }

          originalValues.put(prop, newValue);
        }
      }

      return merged;
    }

    LOGGER.warn(
        "Patch is not supported for type '{}': the mapping contains joins",
        getMapping().getMainSchema().getName());

    return this;
  }

  default List<Integer> getIncrementedRowCounts(List<String> path) {
    List<Integer> currentRowCounts = getRowNesting().getOrDefault(path, getDefaultRowCounts(path));
    int currentRowCount = currentRowCounts.get(currentRowCounts.size() - 1);

    currentRowCounts.set(currentRowCounts.size() - 1, currentRowCount + 1);

    return currentRowCounts;
  }

  default List<Integer> getDefaultRowCounts(List<String> path) {
    int currentParentRowCount = getCurrentParentRowCount(path);

    return IntStream.range(0, currentParentRowCount).mapToObj(i -> 0).collect(Collectors.toList());
  }

  default int getCurrentParentRowCount(List<String> path) {
    for (int i = path.size() - 1; i > 0; i--) {
      if (getRowNesting().containsKey(path.subList(0, i))) {
        List<Integer> parentRowCounts = getRowNesting().get(path.subList(0, i));

        return parentRowCounts.get(parentRowCounts.size() - 1);
      }
    }

    return 1;
  }

  default Optional<SqlRowData> getRow(List<String> fullPath, List<Integer> parentRows) {
    return getRows().stream()
        .filter(row -> Objects.equals(row.first().getFullPath(), fullPath))
        .skip(parentRows.isEmpty() ? 0 : parentRows.get(parentRows.size() - 1))
        .findFirst()
        .map(Tuple::second);
  }
}
