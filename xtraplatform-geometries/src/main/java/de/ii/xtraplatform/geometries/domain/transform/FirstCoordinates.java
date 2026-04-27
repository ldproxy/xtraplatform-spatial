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
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.SingleCurve;

public class FirstCoordinates implements GeometryVisitor<PositionList> {

  @Override
  public PositionList visit(Point geometry) {
    Position p = geometry.getValue();
    return PositionList.of(p.getAxes(), p.getCoordinates());
  }

  @Override
  public PositionList visit(SingleCurve geometry) {
    return geometry.getValue();
  }

  @Override
  public PositionList visit(MultiPoint geometry) {
    return geometry.getValue().stream()
        .findFirst()
        .map(mayThrow(geom -> geom.accept(this)))
        .orElse(PositionList.empty(geometry.getAxes()));
  }

  @Override
  public PositionList visit(MultiLineString geometry) {
    return geometry.getValue().stream()
        .findFirst()
        .map(mayThrow(geom -> geom.accept(this)))
        .orElse(PositionList.empty(geometry.getAxes()));
  }

  @Override
  public PositionList visit(Polygon geometry) {
    return geometry.getValue().stream()
        .findFirst()
        .map(mayThrow(geom -> geom.accept(this)))
        .orElse(PositionList.empty(geometry.getAxes()));
  }

  @Override
  public PositionList visit(MultiPolygon geometry) {
    return geometry.getValue().stream()
        .findFirst()
        .map(mayThrow(geom -> geom.accept(this)))
        .orElse(PositionList.empty(geometry.getAxes()));
  }

  @Override
  public PositionList visit(GeometryCollection geometry) {
    return geometry.getValue().stream()
        .findFirst()
        .map(mayThrow(geom -> geom.accept(this)))
        .orElse(PositionList.empty(geometry.getAxes()));
  }

  @Override
  public PositionList visit(CompoundCurve geometry) {
    return geometry.getValue().stream()
        .findFirst()
        .map(mayThrow(geom -> geom.accept(this)))
        .orElse(PositionList.empty(geometry.getAxes()));
  }

  @Override
  public PositionList visit(CurvePolygon geometry) {
    return geometry.getValue().stream()
        .findFirst()
        .map(mayThrow(geom -> geom.accept(this)))
        .orElse(PositionList.empty(geometry.getAxes()));
  }

  @Override
  public PositionList visit(MultiCurve geometry) {
    return geometry.getValue().stream()
        .findFirst()
        .map(mayThrow(geom -> geom.accept(this)))
        .orElse(PositionList.empty(geometry.getAxes()));
  }

  @Override
  public PositionList visit(MultiSurface geometry) {
    return geometry.getValue().stream()
        .findFirst()
        .map(mayThrow(geom -> geom.accept(this)))
        .orElse(PositionList.empty(geometry.getAxes()));
  }

  @Override
  public PositionList visit(PolyhedralSurface geometry) {
    return geometry.getValue().stream()
        .findFirst()
        .map(mayThrow(geom -> geom.accept(this)))
        .orElse(PositionList.empty(geometry.getAxes()));
  }
}
