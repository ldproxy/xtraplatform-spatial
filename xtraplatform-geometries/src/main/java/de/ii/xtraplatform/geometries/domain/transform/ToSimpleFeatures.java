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
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.MultiCurve;
import de.ii.xtraplatform.geometries.domain.MultiLineString;
import de.ii.xtraplatform.geometries.domain.MultiPoint;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.MultiSurface;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.PolyhedralSurface;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import java.util.Optional;
import java.util.OptionalDouble;

public class ToSimpleFeatures implements GeometryTransformer {

  private final OptionalDouble maxDeviation;

  public ToSimpleFeatures() {
    this.maxDeviation = OptionalDouble.empty();
  }

  public ToSimpleFeatures(double maxDeviation) {
    this.maxDeviation = OptionalDouble.of(maxDeviation);
  }

  @Override
  public Geometry<?> visit(Point geometry) {
    return geometry;
  }

  @Override
  public Geometry<?> visit(SingleCurve geometry) {
    if (geometry.getType() == GeometryType.CIRCULAR_STRING) {
      double maxDev =
          maxDeviation.orElseGet(
              () -> {
                double[][] minMax = geometry.accept(new MinMaxDeriver());
                return (Math.sqrt(
                        Math.pow(
                            minMax[1][0] - minMax[0][0],
                            2 + Math.pow(minMax[1][1] - minMax[0][1], 2))))
                    / 1000.0;
              });
      return LineString.of(
          PositionList.of(
              geometry.getAxes(),
              ArcInterpolator.interpolateArcString(
                  geometry.getValue().getCoordinates(), geometry.getAxes().size(), maxDev)),
          geometry.getCrs());
    }
    return geometry;
  }

  @Override
  public Geometry<?> visit(MultiPoint geometry) {
    return geometry;
  }

  @Override
  public Geometry<?> visit(MultiLineString geometry) {
    return geometry;
  }

  @Override
  public Geometry<?> visit(Polygon geometry) {
    return geometry;
  }

  @Override
  public Geometry<?> visit(MultiPolygon geometry) {
    return geometry;
  }

  @Override
  public Geometry<?> visit(CompoundCurve geometry) {
    Optional<LineString> concat =
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .filter(LineString.class::isInstance)
            .map(LineString.class::cast)
            .reduce(
                (l1, l2) ->
                    LineString.of(
                        PositionList.concat(l1.getValue(), l2.getValue()), geometry.getCrs()));
    return concat.orElse(LineString.empty(geometry.getAxes()));
  }

  @Override
  public Geometry<?> visit(CurvePolygon geometry) {
    return Polygon.of(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .filter(LineString.class::isInstance)
            .map(LineString.class::cast)
            .map(LineString::getValue)
            .toList(),
        geometry.getCrs());
  }

  @Override
  public Geometry<?> visit(MultiCurve geometry) {
    return MultiLineString.of(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .filter(LineString.class::isInstance)
            .map(LineString.class::cast)
            .toList(),
        geometry.getCrs());
  }

  @Override
  public Geometry<?> visit(MultiSurface geometry) {
    return MultiPolygon.of(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .filter(Polygon.class::isInstance)
            .map(Polygon.class::cast)
            .toList(),
        geometry.getCrs());
  }

  @Override
  public Geometry<?> visit(PolyhedralSurface geometry) {
    return MultiPolygon.of(geometry.getValue(), geometry.getCrs());
  }
}
