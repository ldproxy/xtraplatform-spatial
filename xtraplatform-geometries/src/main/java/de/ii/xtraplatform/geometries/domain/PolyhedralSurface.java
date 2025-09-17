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
public interface PolyhedralSurface extends Surface<List<Polygon>>, CompositeGeometry<Polygon> {

  static PolyhedralSurface empty(Axes axes) {
    return ImmutablePolyhedralSurface.builder().axes(axes).value(List.of()).build();
  }

  static PolyhedralSurface of(List<Polygon> polygons) {
    return PolyhedralSurface.of(polygons, false);
  }

  static PolyhedralSurface of(List<Polygon> polygons, boolean closed) {
    return ImmutablePolyhedralSurface.builder()
        .crs(
            polygons.stream()
                .filter(g -> !g.isEmpty())
                .map(Geometry::getCrs)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst())
        .value(polygons.stream().filter(ls -> !ls.isEmpty()).toList())
        .isClosed(closed)
        .build();
  }

  static PolyhedralSurface of(List<Polygon> polygons, boolean closed, Optional<EpsgCrs> crs) {
    return ImmutablePolyhedralSurface.builder()
        .crs(crs)
        .value(polygons.stream().filter(ls -> !ls.isEmpty()).toList())
        .isClosed(closed)
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

  @Value.Derived
  @Value.Auxiliary
  default int getNumPolygons() {
    return getValue().size();
  }

  @Override
  @Value.Derived
  default GeometryType getType() {
    return GeometryType.POLYHEDRAL_SURFACE;
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
