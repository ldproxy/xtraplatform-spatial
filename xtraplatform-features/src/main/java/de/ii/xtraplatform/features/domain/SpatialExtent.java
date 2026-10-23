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

/** Spatial extent in native CRS. */
@Value.Immutable
@JsonDeserialize(builder = ImmutableSpatialExtent.Builder.class)
public interface SpatialExtent {

  @Nullable
  Double getXmin();

  @Nullable
  Double getYmin();

  @Nullable
  Double getZmin();

  @Nullable
  Double getXmax();

  @Nullable
  Double getYmax();

  @Nullable
  Double getZmax();

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
