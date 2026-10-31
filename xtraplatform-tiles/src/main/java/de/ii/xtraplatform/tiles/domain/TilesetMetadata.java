/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.tiles.domain.WithCenter.LonLat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true)
public interface TilesetMetadata extends WithTmsLevels {

  Set<TilesFormat> getEncodings();

  @Override
  Map<String, MinMax> getLevels();

  Optional<LonLat> getCenter();

  Optional<BoundingBox> getBounds();

  List<FeatureSchema> getVectorSchemas();

  Optional<String> getStyleId();

  @JsonIgnore
  @Value.Derived
  default Set<String> getTileMatrixSets() {
    return getLevels().keySet();
  }

  @JsonIgnore
  @Value.Derived
  default boolean isVector() {
    return getEncodings().contains(TilesFormat.MVT);
  }

  @JsonIgnore
  @Value.Derived
  default boolean isRaster() {
    return getEncodings().contains(TilesFormat.JPEG)
        || getEncodings().contains(TilesFormat.PNG)
        || getEncodings().contains(TilesFormat.WebP)
        || getEncodings().contains(TilesFormat.TIFF);
  }

  @Value.Check
  default void checkTileset() {
    Preconditions.checkState(
        !getTileMatrixSets().isEmpty(),
        "There is no tileset, because no tile matrix set has been configured. Found: %s",
        this.toString());
    Preconditions.checkState(
        !getEncodings().isEmpty(),
        "There is no tileset, because no tile encoding has been configured. Found: %s",
        this.toString());
  }
}
