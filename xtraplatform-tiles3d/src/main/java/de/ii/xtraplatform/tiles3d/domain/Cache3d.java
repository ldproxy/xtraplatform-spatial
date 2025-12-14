/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Range;
import de.ii.xtraplatform.base.domain.MapStreams;
import de.ii.xtraplatform.tiles.domain.MinMax;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Locale;
import java.util.Map;
import org.immutables.value.Value;

/**
 * @langEn ### Cache
 *     <p>There is currently only one cache types:
 *     <p><code>
 * - `DYNAMIC` A dynamic cache can be computed through seeding, but it will also cache matching tiles that are requested by a consumer. Tiles will be made available as soon as they are computed. If some or all tiles are re-seeded, they are deleted first.
 *   Therefore a dynamic cache might be inconsistent and may cause client errors during (re-)seeding.
 *     </code>
 * @langDe ### Cache
 *     <p>Es gibt aktuell nur einen Cache-Type:
 *     <p><code>
 * - `DYNAMIC` Ein dynamischer Cache kann durch Seeding berechnet werden, aber er speichert auch passende Kacheln, die von Konsumenten angefragt werden. Kacheln werden verfügbar gemacht, sobald sie berechnet sind. Wenn einige oder alle Kacheln neu berechnet werden sollen, werden sie zunächst gelöscht.
 *   Daher kann ein dynamischer Cache inkonsistent sein und kann während des (Re-)Seedings Client-Fehler auslösen.
 *       </code>
 *     <p>
 */
@Value.Immutable
@JsonDeserialize(builder = ImmutableCache3d.Builder.class)
public interface Cache3d {
  enum Type {
    DYNAMIC
  // ,IMMUTABLE
  ;

    public String getSuffix() {
      return this.name().substring(0, 3).toLowerCase(Locale.ROOT);
    }
  }

  /**
   * @langEn Always `DYNAMIC`.
   * @langDe Immer `DYNAMIC`.
   * @since v4.6
   */
  @Value.Default
  default Type getType() {
    return Type.DYNAMIC;
  }

  /**
   * @langEn Should this cache be included by the [Seeding](#seeding)?
   * @langDe Soll dieser Cache beim [Seeding](#seeding) berücksichtigt werden?
   * @default true
   * @since v4.6
   */
  @Value.Default
  default boolean getSeeded() {
    return true;
  }

  /**
   * @langEn Zoom levels that should be stored in the cache. Applies to all tilesets that are not
   *     specified in `tilesetLevels`.
   * @langDe Zoomstufen, die von diesem Cache gespeichert werden sollen. Gilt für alle Tilesets, die
   *     nicht in `tilesetLevels` angegeben werden.
   * @default {}
   * @since v4.6
   */
  MinMax getLevels();

  /**
   * @langEn Zoom levels for single tilesets that should be stored in the cache.
   * @langDe Zoomstufen für einzelne Tilesets, die von diesem Cache gespeichert werden sollen.
   * @default {}
   * @since v4.6
   */
  Map<String, MinMax> getTilesetLevels();

  @JsonIgnore
  @Value.Derived
  default Map<String, Range<Integer>> getTmsRanges() {
    return getTmsRanges(getLevels());
  }

  @JsonIgnore
  @Value.Derived
  default Map<String, Map<String, Range<Integer>>> getTilesetTmsRanges() {
    return getTilesetLevels().entrySet().stream()
        .map(entry -> new SimpleImmutableEntry<>(entry.getKey(), getTmsRanges(entry.getValue())))
        .collect(MapStreams.toMap());
  }

  default Map<String, Range<Integer>> getTmsRanges(MinMax levels) {
    return Map.of("default", Range.closed(getLevels().getMin(), getLevels().getMax()));
  }
}
