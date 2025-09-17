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
public interface MultiLineString extends AbstractGeometryCollection<LineString> {

  static MultiLineString empty(Axes axes) {
    return ImmutableMultiLineString.builder().axes(axes).value(List.of()).build();
  }

  static MultiLineString of(List<LineString> lineStrings) {
    return ImmutableMultiLineString.builder()
        .crs(
            lineStrings.stream()
                .filter(g -> !g.isEmpty())
                .map(Geometry::getCrs)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst())
        .value(lineStrings.stream().filter(ls -> !ls.isEmpty()).toList())
        .build();
  }

  static MultiLineString of(List<LineString> lineStrings, Optional<EpsgCrs> crs) {
    return ImmutableMultiLineString.builder()
        .crs(crs)
        .value(lineStrings.stream().filter(ls -> !ls.isEmpty()).toList())
        .build();
  }

  @Override
  default boolean isEmpty() {
    return getValue().isEmpty();
  }

  @Value.Derived
  @Value.Auxiliary
  default boolean isClosed() {
    return getValue().stream().allMatch(LineString::isClosed);
  }

  @Override
  @Value.Derived
  default GeometryType getType() {
    return GeometryType.MULTI_LINE_STRING;
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
