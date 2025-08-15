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
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface CircularString extends SingleCurve {

  static CircularString empty(Axes axes) {
    return ImmutableCircularString.builder().value(PositionList.empty(axes)).build();
  }

  static CircularString of(PositionList positionList) {
    return ImmutableCircularString.builder().value(positionList).build();
  }

  static CircularString of(PositionList positionList, Optional<EpsgCrs> crs) {
    return ImmutableCircularString.builder().crs(crs).value(positionList).build();
  }

  @Value.Default
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
        isEmpty() || (getValue().getNumPositions() > 2 && getValue().getNumPositions() % 2 == 1),
        "A non-empty circular string must have an odd number of positions and at least three positions, found %d positions.",
        getValue().getNumPositions());
  }

  @Override
  @Value.Derived
  default GeometryType getType() {
    return GeometryType.CIRCULAR_STRING;
  }
}
