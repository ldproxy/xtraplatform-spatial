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
public interface LineString extends SingleCurve {

  static LineString empty(Axes axes) {
    return ImmutableLineString.builder().value(PositionList.empty(axes)).build();
  }

  static LineString of(double[] xyCoordinates) {
    return ImmutableLineString.builder().value(PositionList.of(Axes.XY, xyCoordinates)).build();
  }

  static LineString of(double[] xyCoordinates, EpsgCrs crs) {
    return ImmutableLineString.builder()
        .crs(crs)
        .value(PositionList.of(Axes.XY, xyCoordinates))
        .build();
  }

  static LineString of(PositionList positionList) {
    return ImmutableLineString.builder().value(positionList).build();
  }

  static LineString of(PositionList positionList, Optional<EpsgCrs> crs) {
    return ImmutableLineString.builder().crs(crs).value(positionList).build();
  }

  static LineString of(List<Position> positions) {
    if (positions.isEmpty()) {
      return empty(Axes.XY);
    }
    Axes axes = positions.get(0).getAxes();
    double[] coordinates = new double[axes.size() * positions.size()];
    for (int i = 0; i < positions.size(); i++) {
      Position position = positions.get(i);
      System.arraycopy(
          position.getCoordinates(),
          0,
          coordinates,
          i * position.getAxes().size(),
          position.getAxes().size());
    }
    return ImmutableLineString.builder().value(PositionList.of(axes, coordinates)).build();
  }

  @Value.Derived
  @Value.Auxiliary
  default Axes getAxes() {
    return getValue().getAxes();
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
        getValue().getNumPositions() != 1,
        "A non-empty line string must have at least two positions, found %s positions.",
        getValue().getNumPositions());
  }

  @Override
  @Value.Derived
  default GeometryType getType() {
    return GeometryType.LINE_STRING;
  }
}
