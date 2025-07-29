/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import de.ii.xtraplatform.crs.domain.EpsgCrs;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface Point extends Primitive<Position> {

  static Point empty(Axes axes) {
    return ImmutablePoint.builder().value(Position.empty(axes)).build();
  }

  static Point of(Position pos) {
    return ImmutablePoint.builder().value(pos).build();
  }

  static Point of(Position pos, Optional<EpsgCrs> crs) {
    return ImmutablePoint.builder().crs(crs).value(pos).build();
  }

  @Value.Derived
  @Value.Auxiliary
  default Axes getAxes() {
    return getValue().getAxes();
  }

  @Override
  @Value.Derived
  default GeometryType getType() {
    return GeometryType.POINT;
  }

  @Override
  @Value.Derived
  @Value.Auxiliary
  default boolean isEmpty() {
    return getValue().isEmpty();
  }
}
