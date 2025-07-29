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
public interface MultiPoint extends AbstractGeometryCollection<Point> {

  static MultiPoint empty(Axes axes) {
    return ImmutableMultiPoint.builder().axes(axes).value(List.of()).build();
  }

  static MultiPoint of(List<Point> points) {
    return ImmutableMultiPoint.builder()
        .value(points.stream().filter(pt -> !pt.isEmpty()).toList())
        .build();
  }

  static MultiPoint of(List<Point> points, Optional<EpsgCrs> crs) {
    return ImmutableMultiPoint.builder()
        .crs(crs)
        .value(points.stream().filter(pt -> !pt.isEmpty()).toList())
        .build();
  }

  @Override
  default boolean isEmpty() {
    return getValue().isEmpty();
  }

  @Override
  @Value.Derived
  default GeometryType getType() {
    return GeometryType.MULTI_POINT;
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
