/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

import de.ii.xtraplatform.geometries.domain.PositionList.Interpolation;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;

/**
 * Adds a constant offset to the easting (first ordinate) of every position. Used to convert between
 * coordinate forms of the same CRS that differ only in the false easting — e.g. German Gauss-Krüger
 * coordinates written without the zone prefix (false easting 500000) versus the EPSG definition
 * with the zone-prefixed false easting (e.g. 3500000 for EPSG:5677): the difference of 3000000 is
 * added on input and subtracted (negative difference) on output.
 */
@Value.Immutable
public abstract class EastingShift implements CoordinatesTransformation {

  @Value.Parameter
  protected abstract double getDifference();

  @Override
  public double[] onCoordinates(
      double[] coordinates,
      int length,
      int dimension,
      Optional<Interpolation> interpolation,
      OptionalInt minNumberOfPositions)
      throws IOException {
    double[] shifted = Arrays.copyOf(coordinates, length);
    for (int i = 0; i < length; i += dimension) {
      // round to micrometres to cancel the floating-point noise the addition introduces
      // (e.g. 3446104.62 - 3000000 = 446104.6200000001)
      shifted[i] = Math.rint((shifted[i] + getDifference()) * 1e6) / 1e6;
    }

    if (getNext().isEmpty()) {
      return shifted;
    }
    return getNext()
        .get()
        .onCoordinates(shifted, length, dimension, interpolation, minNumberOfPositions);
  }
}
