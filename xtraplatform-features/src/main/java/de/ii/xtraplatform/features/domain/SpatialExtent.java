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
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Spatial extent. The coordinates are in the native CRS of the feature provider for providers and
 * types, and in CRS84 (longitude/latitude) for tilesets.
 */
@Value.Immutable
@JsonDeserialize(builder = ImmutableSpatialExtent.Builder.class)
public interface SpatialExtent {

  /**
   * @langEn Minimum x coordinate.
   * @langDe Minimale x-Koordinate.
   * @since v4.9
   */
  @Nullable
  Double getXmin();

  /**
   * @langEn Minimum y coordinate.
   * @langDe Minimale y-Koordinate.
   * @since v4.9
   */
  @Nullable
  Double getYmin();

  /**
   * @langEn Minimum z coordinate, optional.
   * @langDe Minimale z-Koordinate, optional.
   * @since v4.9
   */
  @Nullable
  Double getZmin();

  /**
   * @langEn Maximum x coordinate.
   * @langDe Maximale x-Koordinate.
   * @since v4.9
   */
  @Nullable
  Double getXmax();

  /**
   * @langEn Maximum y coordinate.
   * @langDe Maximale y-Koordinate.
   * @since v4.9
   */
  @Nullable
  Double getYmax();

  /**
   * @langEn Maximum z coordinate, optional.
   * @langDe Maximale z-Koordinate, optional.
   * @since v4.9
   */
  @Nullable
  Double getZmax();

  /**
   * @langEn Set to `true` to compute the extent from the data instead of inheriting a fixed extent
   *     from the parent level (provider for types, tileset defaults for tilesets). Must not be
   *     combined with coordinates; `false` is not allowed.
   * @langDe Auf `true` setzen, um den Extent aus den Daten zu berechnen, statt einen festen Extent
   *     der übergeordneten Ebene (Provider für Types, Tileset-Defaults für Tilesets) zu erben. Darf
   *     nicht mit Koordinaten kombiniert werden; `false` ist nicht erlaubt.
   * @since v4.9
   */
  @Nullable
  Boolean getComputed();

  @Value.Check
  default void checkExclusiveComputed() {
    boolean hasCoordinates =
        getXmin() != null
            || getYmin() != null
            || getZmin() != null
            || getXmax() != null
            || getYmax() != null
            || getZmax() != null;
    boolean autoCompute = Boolean.TRUE.equals(getComputed());

    Preconditions.checkState(
        !Boolean.FALSE.equals(getComputed()),
        "SpatialExtent: 'computed' may only be true, omit it for a fixed extent.");
    Preconditions.checkState(
        !(hasCoordinates && autoCompute),
        "SpatialExtent: 'computed' and explicit coordinates must not be set at the same time.");

    if (hasCoordinates) {
      Preconditions.checkState(
          getXmin() != null && getYmin() != null && getXmax() != null && getYmax() != null,
          "SpatialExtent: xmin, ymin, xmax, ymax are required when coordinates are used.");

      Preconditions.checkState(
          (getZmin() == null && getZmax() == null) || (getZmin() != null && getZmax() != null),
          "SpatialExtent: zmin and zmax must both be set or both be absent.");
    }
  }

  default Optional<BoundingBox> toBoundingBox(EpsgCrs nativeCrs) {
    if (getXmin() == null || getYmin() == null || getXmax() == null || getYmax() == null) {
      return Optional.empty();
    }

    if (getZmin() != null && getZmax() != null) {
      return Optional.of(
          BoundingBox.of(
              getXmin(), getYmin(), getZmin(), getXmax(), getYmax(), getZmax(), nativeCrs));
    }

    return Optional.of(BoundingBox.of(getXmin(), getYmin(), getXmax(), getYmax(), nativeCrs));
  }
}
