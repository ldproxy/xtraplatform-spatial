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
public interface Polygon extends SingleSurface<LineString> {

  static Polygon empty(Axes axes) {
    return ImmutablePolygon.builder().value(List.of()).axes(axes).build();
  }

  static Polygon of(List<PositionList> rings) {
    return ImmutablePolygon.builder()
        .value(rings.stream().map(LineString::of).filter(ring -> !ring.isEmpty()).toList())
        .build();
  }

  static Polygon of(List<PositionList> rings, Optional<EpsgCrs> crs) {
    return ImmutablePolygon.builder()
        .crs(crs)
        .value(
            rings.stream()
                .map(ring -> LineString.of(ring, crs))
                .filter(ring -> !ring.isEmpty())
                .toList())
        .build();
  }

  @Value.Default
  default Axes getAxes() {
    if (isEmpty()) {
      return Axes.XY;
    }
    return getValue().get(0).getAxes();
  }

  @Override
  @Value.Derived
  @Value.Auxiliary
  default boolean isEmpty() {
    return getValue().isEmpty();
  }

  @Value.Check
  default void check() {
    Preconditions.checkState(
        isEmpty() || !getValue().get(0).isEmpty(),
        "A non-empty polygon must have at least an outer ring that is not empty.");
    Preconditions.checkState(
        getValue().stream().allMatch(SingleCurve::isClosed),
        "All rings must be closed. Not closed: %s",
        getValue().stream().filter(c -> !c.isClosed()).toList());
    Preconditions.checkArgument(
        getValue().stream().allMatch(g -> g.getAxes().equals(getAxes())),
        "All geometries must have the same axes.");
    Preconditions.checkArgument(
        getValue().stream()
            .allMatch(
                g -> (g.getCrs().isEmpty() && getCrs().isEmpty()) || (g.getCrs().equals(getCrs()))),
        "All geometries must have the same CRS.");
  }

  @Override
  @Value.Derived
  default GeometryType getType() {
    return GeometryType.POLYGON;
  }
}
