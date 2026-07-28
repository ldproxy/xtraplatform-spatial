/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.domain.FeatureStoreMultiplicityTracker;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zahnen
 */
public class SqlMultiplicityTracker implements FeatureStoreMultiplicityTracker {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlMultiplicityTracker.class);

  private final Map<List<String>, Object> currentIds;
  private final Map<List<String>, Integer> currentMultiplicities;
  private final Map<List<String>, Set<List<String>>> children;

  public SqlMultiplicityTracker(List<List<String>> multiTables) {
    this.currentIds = new HashMap<>();
    this.currentMultiplicities = new HashMap<>();
    this.children = new LinkedHashMap<>();

    // TODO: test with geoval
    multiTables.forEach(
        table -> {
          /*for (int i = 0; i < table.size(); i++) {
              List<String> subPath = table.subList(0, i + 1);
              currentIds.put(subPath, null);
          }*/
          List<String> subPath = table.subList(0, 1);
          currentIds.put(subPath, null);
          currentIds.put(table, null);
        });
  }

  @Override
  public void reset() {
    currentMultiplicities.clear();
    children.clear();

    currentIds
        .keySet()
        .forEach(
            table -> {
              currentIds.put(table, null);
            });
  }

  @Override
  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops", "PMD.CognitiveComplexity"})
  public void track(List<String> path, List<Comparable<?>> ids) {
    int multiplicityIndex = 0;
    boolean increased = false;
    List<String> increasedMultiplicityKey = null;
    List<List<String>> parentTables = new ArrayList<>();

    for (int i = 0; i < path.size(); i++) {
      List<String> table = path.subList(0, i + 1);

      if (currentIds.containsKey(table)) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("TRACKER {} {} {}", table, multiplicityIndex, ids);
        }

        Object id = ids.get(multiplicityIndex);
        if (increased) {
          currentIds.put(table, id);
          currentMultiplicities.put(table, 1);
        } else if (currentIds.containsKey(table) && !Objects.equals(id, currentIds.get(table))) {
          currentIds.put(table, id);
          currentMultiplicities.put(table, currentMultiplicities.getOrDefault(table, 0) + 1);
          increased = true;
          increasedMultiplicityKey = table;
        } else {
          currentMultiplicities.putIfAbsent(table, 1);
        }

        children.computeIfAbsent(table, ignored -> new HashSet<>());
        if (multiplicityIndex > 0) {
          parentTables.forEach(parent -> children.get(parent).add(table));
        }
        parentTables.add(table);

        multiplicityIndex++;
      }
    }

    // reset children that are not on the current path
    if (increased) {
      children
          .get(increasedMultiplicityKey)
          .forEach(
              child -> {
                if (!parentTables.contains(child)) {
                  currentMultiplicities.remove(child);
                }
              });
    }
  }

  @Override
  public List<Integer> getMultiplicitiesForPath(List<String> path) {
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < path.size(); i++) {
      List<String> subPath = path.subList(0, i + 1);
      if (currentIds.containsKey(subPath)) {
        indexes.add(currentMultiplicities.getOrDefault(subPath, 1));
      }
    }
    return indexes;
  }
}
