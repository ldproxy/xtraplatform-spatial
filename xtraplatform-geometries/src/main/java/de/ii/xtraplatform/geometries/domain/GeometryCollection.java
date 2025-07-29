/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import com.google.common.base.Preconditions;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface GeometryCollection extends AbstractGeometryCollection<Geometry<?>> {

  static GeometryCollection empty(Axes axes) {
    return ImmutableGeometryCollection.builder().axes(axes).value(List.of()).build();
  }

  static GeometryCollection of(List<Geometry<?>> geometries) {
    return ImmutableGeometryCollection.builder()
        .value(geometries.stream().filter(g -> !g.isEmpty()).toList())
        .build();
  }

  static GeometryCollection of(List<Geometry<?>> geometries, Optional<EpsgCrs> crs) {
    return ImmutableGeometryCollection.builder()
        .crs(crs)
        .value(geometries.stream().filter(g -> !g.isEmpty()).toList())
        .build();
  }

  @Override
  @Value.Derived
  default GeometryType getType() {
    return GeometryType.GEOMETRY_COLLECTION;
  }

  @Value.Check
  default void check() {
    Preconditions.checkArgument(
        getValue().stream().allMatch(g -> g.getAxes().equals(getAxes())),
        "All geometries must have the same axes.");
    Preconditions.checkArgument(
        getValue().stream()
            .allMatch(
                g -> (g.getCrs().isEmpty() && getCrs().isEmpty()) || (g.getCrs().equals(getCrs()))),
        "All geometries must have the same CRS.");
  }
}
