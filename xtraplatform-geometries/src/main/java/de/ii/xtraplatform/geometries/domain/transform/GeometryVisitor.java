/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

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
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import java.util.Optional;

public interface GeometryVisitor<T> {

  default T visit(Geometry<?> geometry) {

    Optional<T> defaultResult = initAndCheckGeometry(geometry);
    if (defaultResult.isPresent()) {
      return defaultResult.get();
    }

    if (geometry instanceof Point) {
      return visit((Point) geometry);
    } else if (geometry instanceof MultiPoint) {
      return visit((MultiPoint) geometry);
    } else if (geometry instanceof SingleCurve) {
      return visit((SingleCurve) geometry);
    } else if (geometry instanceof MultiLineString) {
      return visit((MultiLineString) geometry);
    } else if (geometry instanceof Polygon) {
      return visit((Polygon) geometry);
    } else if (geometry instanceof MultiPolygon) {
      return visit((MultiPolygon) geometry);
    } else if (geometry instanceof CompoundCurve) {
      return visit((CompoundCurve) geometry);
    } else if (geometry instanceof CurvePolygon) {
      return visit((CurvePolygon) geometry);
    } else if (geometry instanceof MultiCurve) {
      return visit((MultiCurve) geometry);
    } else if (geometry instanceof MultiSurface) {
      return visit((MultiSurface) geometry);
    } else if (geometry instanceof PolyhedralSurface) {
      return visit((PolyhedralSurface) geometry);
    } else if (geometry instanceof GeometryCollection) {
      return visit((GeometryCollection) geometry);
    }

    throw new IllegalStateException();
  }

  default Optional<T> initAndCheckGeometry(Geometry<?> geometry) {
    return Optional.empty();
  }

  T visit(Point geometry);

  T visit(MultiPoint geometry);

  T visit(SingleCurve geometry);

  T visit(MultiLineString geometry);

  T visit(Polygon geometry);

  T visit(MultiPolygon geometry);

  T visit(GeometryCollection geometry);

  T visit(CompoundCurve geometry);

  T visit(CurvePolygon geometry);

  T visit(MultiCurve geometry);

  T visit(MultiSurface geometry);

  T visit(PolyhedralSurface geometry);
}
