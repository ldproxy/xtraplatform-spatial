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
public interface CurvePolygon extends SingleSurface<Curve<?>> {

  static CurvePolygon empty(Axes axes) {
    return ImmutableCurvePolygon.builder().axes(axes).value(List.of()).build();
  }

  static CurvePolygon of(List<Curve<?>> rings) {
    return ImmutableCurvePolygon.builder()
        .value(rings.stream().filter(ls -> !ls.isEmpty()).toList())
        .build();
  }

  static CurvePolygon of(List<Curve<?>> rings, Optional<EpsgCrs> crs) {
    return ImmutableCurvePolygon.builder()
        .crs(crs)
        .value(rings.stream().filter(ls -> !ls.isEmpty()).toList())
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
    Preconditions.checkState(!getValue().isEmpty(), "A curve polygon must have at least one ring.");
    Preconditions.checkState(
        getValue().stream().allMatch(Curve::isClosed), "All rings must be closed.");
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
    return GeometryType.CURVE_POLYGON;
  }
}
