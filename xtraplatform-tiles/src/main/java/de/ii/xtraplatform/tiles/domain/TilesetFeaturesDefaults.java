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
import de.ii.xtraplatform.features.domain.SpatialExtent;
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
   * @langEn Optional fixed spatial extent for this tileset in native CRS.
   * @langDe Optionaler fester räumlicher Extent für dieses Tileset im nativen CRS.
   */
  Optional<SpatialExtent> getExtent();
}
