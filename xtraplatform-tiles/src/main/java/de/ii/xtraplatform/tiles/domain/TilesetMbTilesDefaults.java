/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.tiles.domain.ImmutableMinMax.Builder;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTilesetMbTilesDefaults.Builder.class)
public interface TilesetMbTilesDefaults extends TilesetCommonDefaults {

  @DocIgnore
  @Override
  Optional<LonLat> getCenter();

  @DocIgnore
  @Override
  BuildableMap<MinMax, Builder> getLevels();

  /**
   * @langEn Tile Matrix Set of the tiles in the MBTiles file.
   * @langDe Kachelschema der Kacheln in der MBTiles-Datei.
   * @default WebMercatorQuad
   * @since v4.0
   */
  @Value.Default
  default String getTileMatrixSet() {
    return "WebMercatorQuad";
  }

  /**
   * @langEn Optional fixed spatial extent for this tileset. If set, this value is used as the clip
   *     bounding box instead of falling back to MBTiles metadata.
   * @langDe Optionaler fester räumlicher Extent für dieses Tileset. Wenn gesetzt, wird dieser Wert
   *     als Clip-BoundingBox verwendet, statt auf MBTiles-Metadaten zurückzufallen.
   */
  Optional<BoundingBox> getExtent();

  /**
   * @langEn If true, the spatial extent will always be computed from data, even if a fixed value is
   *     set globally. If false, a fixed value is always used. If not set, the global or provider
   *     logic applies.
   * @langDe Wenn true, wird der räumliche Extent immer aus den Daten berechnet, auch wenn global
   *     ein fester Wert gesetzt ist. Wenn false, wird immer ein fixer Wert verwendet. Wenn nicht
   *     gesetzt, gilt die globale oder Provider-Logik.
   */
  default Optional<Boolean> getSpatialExtentComputed() {
    return Optional.empty();
  }
}
