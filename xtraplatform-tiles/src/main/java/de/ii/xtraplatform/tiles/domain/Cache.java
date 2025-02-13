/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Locale;
import java.util.Map;
import org.immutables.value.Value;

/**
 * @langEn ### Cache
 *     <p>There are two different cache types:
 *     <p><code>
 * - `IMMUTABLE` An immutable cache can only be created through seeding. It will only be made available once it is complete. If some or all tiles are re-seeded, then again the new version will only be made available once it is complete. In the meantime the old version is still provided to consumers.
 *   Therefore an immutable cache is always consistent and, once the initial seeding has completed, it guarantees uninterrupted operation.
 * - `DYNAMIC` A dynamic cache can be computed through seeding, but it will also cache matching tiles that are requested by a consumer. Tiles will be made available as soon as they are computed. If some or all tiles are re-seeded, they are deleted first.
 *   Therefore a dynamic cache might be inconsistent and may cause client errors during (re-)seeding.
 *     </code>
 *     <p>In general the usage of immutable caches is recommended. For higher zoom levels a
 *     complementary non-seeded dynamic cache might make sense to reduce seeding time.
 *     <p>
 *     <p>There are two different storage types:
 *     <p><code>
 * - `PER_JOB` Tiles are stored in one file per seeding job, see `jobSize` in [Seeding](#seeding). This is the recommended setting for seeded caches.
 * - `PER_TILE` (or deprecated `PLAIN`) Every tile is stored in a separate file.
 * - `PER_TILESET` (or deprecated `MBTILES`) Tiles are stored in a single file per tiling scheme.
 *     </code>
 *     <p>
 * @langDe ### Cache
 *     <p>Es gibt zwei verschiedene Cache-Typen:
 *     <p><code>
 * - `IMMUTABLE` Ein unveränderbarer Cache kann nur durch Seeding erzeugt werden. Er wird erst verfügbar gemacht, wenn er vollständig ist. Wenn einige oder alle Kacheln neu berechnet werden sollen, wird auch die neue Version erst verfügbar gemacht, wenn sie vollstänidg ist. In der Zwischenzeit wird weiter die alte Version verwendet.
 *   Daher ist ein unveränderbarer Cache immer konsistent und garantiert unterbrechungsfreien Betrieb, sobald das initiale Seeding abgeschlossen ist.
 * - `DYNAMIC` Ein dynamischer Cache kann durch Seeding berechnet werden, aber er speichert auch passende Kacheln, die von Konsumenten angefragt werden. Kacheln werden verfügbar gemacht, sobald sie berechnet sind. Wenn einige oder alle Kacheln neu berechnet werden sollen, werden sie zunächst gelöscht.
 *   Daher kann ein dynamischer Cache inkonsistent sein und kann während des (Re-)Seedings Client-Fehler auslösen.
 *       </code>
 *     <p>Im allgemeinen wird die Verwendung von unveränderbaren Caches empfohlen. Für höhere
 *     Zoom-Level kann ein komplementärer nicht-geseedeter dynamischer Cache Sinn machen, um die
 *     Seeding-Zeit zu reduzieren.
 *     <p>
 *     <p>Es gibt zwei verschiedene Storage-Typen:
 *     <p><code>
 * - `PER_JOB` Kacheln werden in einer Datei pro Seeding-Job gespeichert, siehe `jobSize` in [Seeding](#seeding). Dies ist die empfohlene Einstellung für geseedete Caches.
 * - `PER_TILE` (oder deprecated `PLAIN`) Jede Kachel wird in einer separaten Datei gespeichert.
 * - `PER_TILESET` (oder deprecated `MBTILES`) Kacheln werden in einer einzigen Datei pro Kachelschema gespeichert.
 *      </code>
 *     <p>
 */
@Value.Immutable
@JsonDeserialize(builder = ImmutableCache.Builder.class)
public interface Cache extends WithTmsLevels, WithTilesetTmsLevels {
  enum Type {
    DYNAMIC,
    IMMUTABLE;

    public String getSuffix() {
      return this.name().substring(0, 3).toLowerCase(Locale.ROOT);
    }
  }

  enum Storage {
    PER_TILE,
    PER_JOB,
    PER_TILESET,
    @Deprecated(since = "4.1", forRemoval = true)
    PLAIN,
    @Deprecated(since = "4.1", forRemoval = true)
    MBTILES
  }

  /**
   * @langEn Either `IMMUTABLE` or `DYNAMIC`.
   * @langDe Entweder `IMMUTABLE` oder `DYNAMIC`.
   * @since v3.4
   */
  Type getType();

  /**
   * @langEn Either `PLAIN` or `MBTILES`.
   * @langDe Entweder `PLAIN` oder `MBTILES`.
   * @since v3.4
   */
  Storage getStorage();

  /**
   * @langEn Should this cache be included by the [Seeding](#seeding)?
   * @langDe Soll dieser Cache beim [Seeding](#seeding) berücksichtigt werden?
   * @default true
   * @since v3.4
   */
  @Value.Default
  default boolean getSeeded() {
    return true;
  }

  /**
   * @langEn Tiling schemes and zoom levels that should be stored in the cache. Applies to all
   *     tilesets that are not specified in `tilesetLevels`.
   * @langDe Kachelschemas und Zoomstufen, die von diesem Cache gespeichert werden sollen. Gilt für
   *     alle Tilesets, die nicht in `tilesetLevels` angegeben werden.
   * @default {}
   * @since v3.4
   */
  @Override
  Map<String, MinMax> getLevels();

  /**
   * @langEn Tiling schemes and zoom levels for single tilesets that should be stored in the cache.
   * @langDe Kachelschemas und Zoomstufen für einzelne Tilesets, die von diesem Cache gespeichert
   *     werden sollen.
   * @default {}
   * @since v3.4
   */
  @Override
  Map<String, Map<String, MinMax>> getTilesetLevels();
}
