/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

import static de.ii.xtraplatform.base.domain.util.LambdaWithException.mayThrow;

import de.ii.xtraplatform.geometries.domain.CompoundCurve;
import de.ii.xtraplatform.geometries.domain.CurvePolygon;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryCollection;
import de.ii.xtraplatform.geometries.domain.MultiCurve;
import de.ii.xtraplatform.geometries.domain.MultiLineString;
import de.ii.xtraplatform.geometries.domain.MultiPoint;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.MultiSurface;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.PolyhedralSurface;
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class MinMaxDeriver implements GeometryVisitor<double[][]> {

  @Override
  public double[][] visit(Point geometry) {
    if (geometry.isEmpty()) {
      return null;
    }
    int dimensions = geometry.getAxes().size();
    final double[][] minMax = initMinMax(dimensions);
    Position p = geometry.getValue();
    for (int i = 0; i < p.getAxes().size(); i++) {
      minMax[0][i] = p.getCoordinates()[i];
      minMax[1][i] = p.getCoordinates()[i];
    }
    return minMax;
  }

  @Override
  public double[][] visit(SingleCurve geometry) {
    if (geometry.isEmpty()) {
      return null;
    }
    int dimensions = geometry.getAxes().size();
    final double[][] minMax = initMinMax(dimensions);

    double[] coordinates = geometry.getValue().getCoordinates();
    IntStream.range(0, geometry.getNumPoints())
        .forEach(
            p -> {
              for (int i = 0; i < dimensions; i++) {
                minMax[0][i] = Math.min(minMax[0][i], coordinates[p * dimensions + i]);
                minMax[1][i] = Math.max(minMax[1][i], coordinates[p * dimensions + i]);
              }
            });

    return minMax;
  }

  @Override
  public double[][] visit(MultiPoint geometry) {
    return fromComponents(geometry);
  }

  @Override
  public double[][] visit(MultiLineString geometry) {
    return fromComponents(geometry);
  }

  @Override
  public double[][] visit(Polygon geometry) {
    return fromComponents(geometry);
  }

  @Override
  public double[][] visit(MultiPolygon geometry) {
    return fromComponents(geometry);
  }

  @Override
  public double[][] visit(GeometryCollection geometry) {
    return fromComponents(geometry);
  }

  @Override
  public double[][] visit(CompoundCurve geometry) {
    return fromComponents(geometry);
  }

  @Override
  public double[][] visit(CurvePolygon geometry) {
    return fromComponents(geometry);
  }

  @Override
  public double[][] visit(MultiCurve geometry) {
    return fromComponents(geometry);
  }

  @Override
  public double[][] visit(MultiSurface geometry) {
    return fromComponents(geometry);
  }

  @Override
  public double[][] visit(PolyhedralSurface geometry) {
    return fromComponents(geometry);
  }

  private static double[][] initMinMax(int dimensions) {
    double[][] minMax = new double[2][dimensions];
    Arrays.fill(minMax[0], Double.MAX_VALUE);
    Arrays.fill(minMax[1], Double.MIN_VALUE);
    return minMax;
  }

  private double[][] fromComponents(Geometry<?> geometry) {
    if (geometry.isEmpty()) {
      return null;
    }

    if (!(geometry.getValue() instanceof List<?>)) {
      throw new IllegalArgumentException(
          "Geometry value must be a List<Geometry<?>>, but was: " + geometry.getValue().getClass());
    }

    int dimensions = geometry.getAxes().size();
    final double[][] minMax = initMinMax(dimensions);
    ((List<?>) geometry.getValue())
        .stream()
            .filter(Geometry.class::isInstance)
            .map(geom -> (Geometry<?>) geom)
            .map(mayThrow(v -> v.accept(this)))
            .forEach(
                mm -> {
                  for (int i = 0; i < dimensions; i++) {
                    minMax[0][i] = Math.min(minMax[0][i], mm[0][i]);
                    minMax[1][i] = Math.max(minMax[1][i], mm[1][i]);
                  }
                });

    return minMax;
  }
}
