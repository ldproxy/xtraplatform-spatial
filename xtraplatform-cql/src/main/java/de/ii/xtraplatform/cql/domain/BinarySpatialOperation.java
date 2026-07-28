/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Locale;
import org.immutables.value.Value;

public interface BinarySpatialOperation extends BinaryOperation2<Spatial> {

  @JsonIgnore
  @Value.Derived
  default SpatialFunction getSpatialOperator() {
    return SpatialFunction.valueOf(getOp().toUpperCase(Locale.ROOT));
  }

  @SuppressWarnings("PMD.CyclomaticComplexity")
  static BinarySpatialOperation of(SpatialFunction operator, Spatial spatial1, Spatial spatial2) {
    switch (operator) {
      case S_INTERSECTS:
        return SIntersects.of(spatial1, spatial2);
      case S_EQUALS:
        return SEquals.of(spatial1, spatial2);
      case S_DISJOINT:
        return SDisjoint.of(spatial1, spatial2);
      case S_TOUCHES:
        return STouches.of(spatial1, spatial2);
      case S_WITHIN:
        return SWithin.of(spatial1, spatial2);
      case S_OVERLAPS:
        return SOverlaps.of(spatial1, spatial2);
      case S_CROSSES:
        return SCrosses.of(spatial1, spatial2);
      case S_CONTAINS:
        return SContains.of(spatial1, spatial2);
    }

    throw new IllegalStateException();
  }

  abstract class Builder<T extends BinarySpatialOperation> extends Operation.Builder<Spatial, T> {}
}
