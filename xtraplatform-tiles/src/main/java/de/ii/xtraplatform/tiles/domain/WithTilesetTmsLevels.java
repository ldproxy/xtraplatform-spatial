/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Range;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface WithTilesetTmsLevels {
  Map<String, Map<String, MinMax>> getTilesetLevels();

  @JsonIgnore
  @Value.Derived
  default Map<String, Map<String, Range<Integer>>> getTilesetTmsRanges() {
    return getTilesetLevels().entrySet().stream()
        .map(entry -> new SimpleImmutableEntry<>(entry.getKey(), getTmsRanges(entry.getValue())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  default Map<String, Range<Integer>> getTmsRanges(Map<String, MinMax> levels) {
    return levels.entrySet().stream()
        .map(
            entry ->
                new SimpleImmutableEntry<>(
                    entry.getKey(),
                    Range.closed(entry.getValue().getMin(), entry.getValue().getMax())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
