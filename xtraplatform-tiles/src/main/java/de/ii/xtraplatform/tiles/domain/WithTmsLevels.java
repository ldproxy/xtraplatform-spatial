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
public interface WithTmsLevels {
  /**
   * @langEn Controls the zoom levels available for each active tiling scheme as well as which zoom
   *     level to use as default.
   * @langDe Steuert die Zoomstufen, die für jedes aktive Kachelschema verfügbar sind sowie welche
   *     Zoomstufe als Default verwendet werden soll.
   * @default { "WebMercatorQuad" : { "min": 0, "max": 23 } }
   * @since v3.4
   */
  Map<String, MinMax> getLevels();

  @JsonIgnore
  @Value.Derived
  default Map<String, Range<Integer>> getTmsRanges() {
    return getLevels().entrySet().stream()
        .map(
            entry ->
                new SimpleImmutableEntry<>(
                    entry.getKey(),
                    Range.closed(entry.getValue().getMin(), entry.getValue().getMax())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
