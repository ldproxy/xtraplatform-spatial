/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

import de.ii.xtraplatform.geometries.domain.CircularString;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.PositionList.Interpolation;
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;

public class CoordinatesTransformer implements GeometryTransformer {

  private final CoordinatesTransformation transformationChain;

  public CoordinatesTransformer(CoordinatesTransformation transformationChain) {
    this.transformationChain = transformationChain;
  }

  @Override
  public Geometry<?> visit(Point geometry) {
    return Point.of(
        Position.of(
            geometry.getAxes(),
            processPositions(
                geometry.getValue().getCoordinates(),
                geometry.getAxes().size(),
                Optional.empty(),
                OptionalInt.empty())),
        geometry.getCrs());
  }

  @Override
  public Geometry<?> visit(SingleCurve geometry) {
    int dimension = geometry.getAxes().size();
    double[] coordinates = geometry.getValue().getCoordinates();
    if (geometry instanceof CircularString) {
      return CircularString.of(
          PositionList.of(
              geometry.getAxes(),
              processPositions(
                  coordinates,
                  dimension,
                  Optional.of(Interpolation.CIRCULAR),
                  OptionalInt.of(geometry.isClosed() ? 4 : 2))),
          geometry.getCrs());
    }
    return LineString.of(
        PositionList.of(
            geometry.getAxes(),
            processPositions(
                coordinates,
                dimension,
                Optional.of(Interpolation.LINE),
                OptionalInt.of(geometry.isClosed() ? 4 : 2))),
        geometry.getCrs());
  }

  // The default methods apply for all other geometry types

  private double[] processPositions(
      double[] coordinates,
      int dimension,
      Optional<Interpolation> interpolation,
      OptionalInt minNumberOfPositions) {
    try {
      return transformationChain.onCoordinates(
          coordinates, coordinates.length, dimension, interpolation, minNumberOfPositions);
    } catch (IOException e) {
      throw new IllegalStateException("Error transforming coordinates.", e);
    }
  }
}
