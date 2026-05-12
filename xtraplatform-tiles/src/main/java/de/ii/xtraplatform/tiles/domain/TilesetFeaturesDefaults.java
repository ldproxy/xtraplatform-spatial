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
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * @langEn ### Tileset Defaults
 *     <p>Defaults that are applied to each [Tileset](#tileset).
 * @langDe ### Tileset Defaults
 *     <p>Defaults die für jedes [Tileset](#tileset) angewendet werden.
 */
@Value.Immutable
@JsonDeserialize(builder = ImmutableTilesetFeaturesDefaults.Builder.class)
public interface TilesetFeaturesDefaults
    extends TilesetCommonDefaults,
        TileGenerationOptions,
        WithFeatureProvider,
        Buildable<TilesetFeaturesDefaults> {
  @Override
  BuildableMap<MinMax, ImmutableMinMax.Builder> getLevels();

  @Override
  default ImmutableTilesetFeaturesDefaults.Builder getBuilder() {
    return new ImmutableTilesetFeaturesDefaults.Builder().from(this);
  }

  abstract class Builder implements BuildableBuilder<TilesetFeaturesDefaults> {}

  /**
   * @langEn Optional fixed spatial extent for this tileset. If set, this value is used as the clip
   *     bounding box instead of computing it.
   * @langDe Optionaler fester räumlicher Extent für dieses Tileset. Wenn gesetzt, wird dieser Wert
   *     als Clip-BoundingBox verwendet, statt ihn zu berechnen.
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
