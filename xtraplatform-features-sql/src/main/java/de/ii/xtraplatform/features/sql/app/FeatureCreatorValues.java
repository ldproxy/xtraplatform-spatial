/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import de.ii.xtraplatform.features.domain.FeatureStoreRelation;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author zahnen
 */
class FeatureCreatorValues {
  final List<FeatureStoreRelation> path;
  final ListMultimap<List<FeatureStoreRelation>, FeatureCreatorValues> relatedValues;
  final Map<List<FeatureStoreRelation>, List<Integer>> rowCounts;
  final Map<String, String> values;
  final Map<String, String> ids;

  FeatureCreatorValues(List<FeatureStoreRelation> path) {
    this.path = path;
    this.relatedValues = ArrayListMultimap.create();
    this.rowCounts = new LinkedHashMap<>();
    this.values = new LinkedHashMap<>();
    this.ids = new LinkedHashMap<>();
  }

  FeatureCreatorValues getRelatedValues(List<FeatureStoreRelation> path, List<Integer> parentRows) {
    int row = parentRows.size() < path.size() ? 0 : parentRows.get(path.size() - 1);

    return getRelatedValues(path, row);
  }

  FeatureCreatorValues getCurrentRelatedValues(List<FeatureStoreRelation> path) {
    if (path.equals(this.path)) {
      return this;
    }

    int row = Optional.ofNullable(relatedValues.get(path)).map(List::size).orElse(0);

    return getRelatedValues(path, row);
  }

  Map<List<FeatureStoreRelation>, List<Integer>> getRowCounts() {
    return rowCounts;
  }

  private FeatureCreatorValues getRelatedValues(List<FeatureStoreRelation> path, int row) {
    if (!relatedValues.containsKey(path)) {
      throw new IllegalStateException(String.format("No values found for path %s", path));
    }

    List<FeatureCreatorValues> values = relatedValues.get(path);

    if (row > values.size() - 1) {
      throw new IllegalStateException(String.format("No values found for row %s of %s", row, path));
    }

    return values.get(row);
  }

  // NOTE: value escaping to SqlSyntax
  void addValue(List<FeatureStoreRelation> path, String attribute, String value) {
    FeatureCreatorValues related = getCurrentRelatedValues(path);
    related.values.put(attribute, value != null ? "'" + value.replaceAll("'", "''") + "'" : null);
  }

  void addRow(List<FeatureStoreRelation> path) {
    if (!relatedValues.containsKey(path)) {
      throw new IllegalStateException(String.format("No values found for path %s", path));
    }

    relatedValues.put(path, new FeatureCreatorValues(path));

    rowCounts.put(path, getIncrementedRowCounts(path));
  }

  private List<Integer> getIncrementedRowCounts(List<FeatureStoreRelation> path) {
    List<Integer> currentRowCounts = rowCounts.getOrDefault(path, getDefaultRowCounts(path));
    int currentRowCount = currentRowCounts.get(currentRowCounts.size() - 1);

    currentRowCounts.add(currentRowCounts.size() - 1, currentRowCount + 1);

    return currentRowCounts;
  }

  private List<Integer> getDefaultRowCounts(List<FeatureStoreRelation> path) {
    int currentParentRowCount = getCurrentParentRowCount(path);

    return IntStream.range(0, currentParentRowCount).mapToObj(i -> 0).collect(Collectors.toList());
  }

  private int getCurrentParentRowCount(List<FeatureStoreRelation> path) {
    for (int i = path.size() - 1; i > 0; i--) {
      if (rowCounts.containsKey(path.subList(0, i))) {
        List<Integer> parentRowCounts = rowCounts.get(path.subList(0, i));

        return parentRowCounts.get(parentRowCounts.size() - 1);
      }
    }

    return 1;
  }
}
