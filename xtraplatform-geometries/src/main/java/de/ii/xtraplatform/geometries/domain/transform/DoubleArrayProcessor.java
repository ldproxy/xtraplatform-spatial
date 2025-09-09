/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

import de.ii.xtraplatform.geometries.domain.PositionList.Interpolation;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;

public interface DoubleArrayProcessor<T> {
  T onCoordinates(
      double[] coordinates,
      int length,
      int dimension,
      Optional<Interpolation> interpolation,
      OptionalInt minNumberOfPositions)
      throws IOException;
}
