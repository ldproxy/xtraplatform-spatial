/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

import de.ii.xtraplatform.geometries.domain.CircularString;
import de.ii.xtraplatform.geometries.domain.CompoundCurve;
import de.ii.xtraplatform.geometries.domain.CurvePolygon;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.PolyhedralSurface;
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.IntStream;

public class ClampToEllipsoid implements GeometryTransformer {

  private final double deltaZ;

  public ClampToEllipsoid() {
    this.deltaZ = Double.NaN;
  }

  public ClampToEllipsoid(double deltaZ) {
    this.deltaZ = deltaZ;
  }

  @Override
  public Optional<Geometry<?>> initAndCheckGeometry(Geometry<?> geometry) {
    if (!geometry.hasZ()) {
      throw new IllegalArgumentException(
          String.format(
              "The geometry must have a Z coordinate to clamp the geometry to the ellipsoid. Axes: %s.",
              geometry.getAxes()));
    }

    if (geometry.isEmpty()) {
      return Optional.empty();
    }

    return Optional.empty();
  }

  @Override
  public Geometry<?> visit(Point geometry) {
    Position p = geometry.getValue();
    double[] newCoordinates = Arrays.copyOf(p.getCoordinates(), p.getCoordinates().length);
    newCoordinates[2] = Double.isNaN(deltaZ) ? 0.0 : p.getCoordinates()[2] - deltaZ;
    return Point.of(Position.of(geometry.getAxes(), newCoordinates), geometry.getCrs());
  }

  @Override
  public Geometry<?> visit(SingleCurve geometry) {
    int dimensions = geometry.getAxes().size();
    double[] coordinates = geometry.getValue().getCoordinates();
    OptionalDouble min =
        Double.isNaN(deltaZ)
            ? IntStream.range(0, geometry.getNumPoints())
                .mapToDouble(p -> coordinates[p * dimensions + 2])
                .min()
            : OptionalDouble.of(deltaZ);
    if (min.isPresent()) {
      double[] newCoordinates = Arrays.copyOf(coordinates, coordinates.length);
      IntStream.range(0, geometry.getNumPoints())
          .forEach(
              p ->
                  newCoordinates[p * dimensions + 2] =
                      newCoordinates[p * dimensions + 2] - min.getAsDouble());
      if (geometry instanceof CircularString) {
        return CircularString.of(
            PositionList.of(geometry.getAxes(), newCoordinates), geometry.getCrs());
      }
      return LineString.of(PositionList.of(geometry.getAxes(), newCoordinates), geometry.getCrs());
    }
    return geometry;
  }

  // The default methods apply for all other geometry types

  @Override
  public Geometry<?> visit(Polygon geometry) {
    if (!Double.isNaN(deltaZ)) {
      return GeometryTransformer.super.visit(geometry);
    }
    return geometry.accept(new ClampToEllipsoid(geometry.accept(new MinMaxDeriver())[0][2]));
  }

  @Override
  public Geometry<?> visit(CompoundCurve geometry) {
    if (!Double.isNaN(deltaZ)) {
      return GeometryTransformer.super.visit(geometry);
    }
    return geometry.accept(new ClampToEllipsoid(geometry.accept(new MinMaxDeriver())[0][2]));
  }

  @Override
  public Geometry<?> visit(CurvePolygon geometry) {
    if (!Double.isNaN(deltaZ)) {
      return GeometryTransformer.super.visit(geometry);
    }
    return geometry.accept(new ClampToEllipsoid(geometry.accept(new MinMaxDeriver())[0][2]));
  }

  @Override
  public Geometry<?> visit(PolyhedralSurface geometry) {
    if (!Double.isNaN(deltaZ)) {
      return GeometryTransformer.super.visit(geometry);
    }
    return geometry.accept(new ClampToEllipsoid(geometry.accept(new MinMaxDeriver())[0][2]));
  }
}
