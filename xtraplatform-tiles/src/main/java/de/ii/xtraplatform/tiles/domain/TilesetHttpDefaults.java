/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.features.domain.Extent;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * @langEn ### Tileset Defaults
 *     <p>Defaults that are applied to each [Tileset](#tileset).
 * @langDe ### Tileset Defaults
 *     <p>Defaults die für jedes [Tileset](#tileset) angewendet werden.
 */
@Value.Immutable
@JsonDeserialize(builder = ImmutableTilesetHttpDefaults.Builder.class)
public interface TilesetHttpDefaults
    extends TilesetCommonDefaults, WithEncodings, Buildable<TilesetHttpDefaults> {
  @Override
  BuildableMap<MinMax, ImmutableMinMax.Builder> getLevels();

  @Override
  default ImmutableTilesetHttpDefaults.Builder getBuilder() {
    return new ImmutableTilesetHttpDefaults.Builder().from(this);
  }

  /**
   * @langEn Fixed extent of the tileset, only `spatial` in CRS84 (longitude/latitude). If not set,
   *     the extent of the tile matrix sets is used.
   * @langDe Fester Extent des Tilesets, nur `spatial` in CRS84 (Länge/Breite). Wenn nicht gesetzt,
   *     wird der Extent der Kachelschemas verwendet.
   * @since v4.9
   */
  Optional<Extent> getExtent();

  @Value.Check
  default void checkExtentIsSpatial() {
    Extent.checkSpatialOnly(getExtent());
  }

  abstract class Builder implements BuildableBuilder<TilesetHttpDefaults> {}
}
