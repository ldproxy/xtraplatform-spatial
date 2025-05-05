/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.immutables.value.Value;

@Value.Modifiable
public interface FeatureDataSql {

  SqlQueryMapping getMapping();

  // TODO
  default void putChildrenIds(String name, String returned) {}

  List<Tuple<SqlQuerySchema, Map<String, String>>> getRows();

  List<Tuple<SqlQuerySchema, AtomicInteger>> getStack();

  default void addRow(SqlQuerySchema table) {
    int newCurrent = -1;

    for (int i = getStack().size() - 1; i >= 0; i--) {
      if (Objects.equals(getStack().get(i).first(), table)) {
        getStack().get(i).second().set(getRows().size());
        newCurrent = i;
        getRows().add(Tuple.of(table, new LinkedHashMap<>()));
        break;
      } else if (table
          .getFullPathAsString()
          .startsWith(getStack().get(i).first().getFullPathAsString())) {
        getStack().add(i + 1, Tuple.of(table, new AtomicInteger(getRows().size())));
        newCurrent = i + 1;
        getRows().add(Tuple.of(table, new LinkedHashMap<>()));
        break;
      }
    }

    if (newCurrent == -1) {
      getStack().add(Tuple.of(table, new AtomicInteger(getRows().size())));
      newCurrent = getStack().size() - 1;
      getRows().add(Tuple.of(table, new LinkedHashMap<>()));
    }

    if (getStack().size() > newCurrent + 1) {
      getStack().subList(newCurrent + 1, getStack().size()).clear();
    }
  }

  default void addColumn(SqlQuerySchema table, SqlQueryColumn column, String value) {
    if (isCurrent(table)) {
      getCurrentRow(table).put(column.getName(), value);
    }
  }

  default boolean isCurrent(SqlQuerySchema table) {
    return !getStack().isEmpty()
        && Objects.equals(getStack().get(getStack().size() - 1).first(), table);
  }

  default Map<String, String> getCurrentRow(SqlQuerySchema table) {
    return getRows().get(getStack().get(getStack().size() - 1).second().get()).second();
  }

  // TODO
  default FeatureDataSql patchWith(FeatureDataSql partial) {
    return this;
  }
}
