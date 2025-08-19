/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import java.util.Arrays;
import org.immutables.value.Value;

public interface SingleCurve extends Curve<PositionList> {

  @Value.Derived
  @Value.Auxiliary
  default int getNumPoints() {
    if (isEmpty()) {
      return 0;
    }
    return getValue().getNumPositions();
  }

  @Override
  @Value.Derived
  @Value.Auxiliary
  default boolean isClosed() {
    if (isEmpty()) {
      return true;
    }
    final int size = getAxes().size();
    final int numPositions = getValue().getNumPositions();
    return Arrays.equals(
        getValue().getCoordinates(),
        0,
        size,
        getValue().getCoordinates(),
        (numPositions - 1) * size,
        numPositions * size);
  }
}
