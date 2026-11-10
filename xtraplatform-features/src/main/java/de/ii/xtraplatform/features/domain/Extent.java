/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.immutables.value.Value;

/** Extent object for spatial and temporal extents. */
@Value.Immutable
@JsonDeserialize(builder = ImmutableExtent.Builder.class)
public interface Extent {
  Optional<SpatialExtent> getSpatial();

  Optional<TemporalExtent> getTemporal();

  /**
   * Each part of the extent replaces the same part of the extent of the parent level (provider for
   * types, API for collections, tileset defaults for tilesets); a part with {@code computed: true}
   * therefore discards an inherited fixed extent.
   */
  static Extent inherit(Optional<Extent> extent, Optional<Extent> parentExtent) {
    return ImmutableExtent.builder()
        .spatial(
            extent.flatMap(Extent::getSpatial).or(() -> parentExtent.flatMap(Extent::getSpatial)))
        .temporal(
            extent.flatMap(Extent::getTemporal).or(() -> parentExtent.flatMap(Extent::getTemporal)))
        .build();
  }

  /** Tiles have only a spatial extent, a temporal extent is rejected. */
  static void checkSpatialOnly(Optional<Extent> extent) {
    Preconditions.checkState(
        extent.flatMap(Extent::getTemporal).isEmpty(),
        "Extent: 'temporal' is not supported for tiles, only 'spatial'.");
  }
}
