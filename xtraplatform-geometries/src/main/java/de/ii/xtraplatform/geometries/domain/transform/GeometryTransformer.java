/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

import static de.ii.xtraplatform.base.domain.util.LambdaWithException.mayThrow;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.geometries.domain.CompoundCurve;
import de.ii.xtraplatform.geometries.domain.Curve;
import de.ii.xtraplatform.geometries.domain.CurvePolygon;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryCollection;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.MultiCurve;
import de.ii.xtraplatform.geometries.domain.MultiLineString;
import de.ii.xtraplatform.geometries.domain.MultiPoint;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.MultiSurface;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.PolyhedralSurface;
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import de.ii.xtraplatform.geometries.domain.Surface;

public interface GeometryTransformer extends GeometryVisitor<Geometry<?>> {

  Geometry<?> visit(Point geometry);

  Geometry<?> visit(SingleCurve geometry);

  @Override
  default Geometry<?> visit(MultiPoint geometry) {
    return MultiPoint.of(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .map(Point.class::cast)
            .toList(),
        geometry.getCrs());
  }

  @Override
  default Geometry<?> visit(MultiLineString geometry) {
    return MultiLineString.of(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .map(LineString.class::cast)
            .toList(),
        geometry.getCrs());
  }

  @Override
  default Geometry<?> visit(Polygon geometry) {
    return Polygon.of(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .map(LineString.class::cast)
            .map(Geometry::getValue)
            .toList(),
        geometry.getCrs());
  }

  @Override
  default Geometry<?> visit(MultiPolygon geometry) {
    return MultiPolygon.of(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .map(Polygon.class::cast)
            .toList(),
        geometry.getCrs());
  }

  @Override
  default Geometry<?> visit(GeometryCollection geometry) {
    return GeometryCollection.of(
        ImmutableList.<Geometry<?>>builder()
            .addAll(geometry.getValue().stream().map(mayThrow(geom -> geom.accept(this))).toList())
            .build(),
        geometry.getCrs());
  }

  @Override
  default Geometry<?> visit(CompoundCurve geometry) {
    return CompoundCurve.of(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .map(SingleCurve.class::cast)
            .toList(),
        geometry.getCrs());
  }

  @Override
  default Geometry<?> visit(CurvePolygon geometry) {
    return CurvePolygon.of(
        ImmutableList.<Curve<?>>builder()
            .addAll(
                geometry.getValue().stream()
                    .map(mayThrow(geom -> geom.accept(this)))
                    .filter(Curve.class::isInstance)
                    .map(
                        geom -> {
                          if (geom instanceof CompoundCurve) {
                            return (CompoundCurve) geom;
                          }
                          return (SingleCurve) geom;
                        })
                    .toList())
            .build(),
        geometry.getCrs());
  }

  @Override
  default Geometry<?> visit(MultiCurve geometry) {
    return MultiCurve.of(
        ImmutableList.<Curve<?>>builder()
            .addAll(
                geometry.getValue().stream()
                    .map(mayThrow(geom -> geom.accept(this)))
                    .filter(Curve.class::isInstance)
                    .map(
                        geom -> {
                          if (geom instanceof CompoundCurve) {
                            return (CompoundCurve) geom;
                          }
                          return (SingleCurve) geom;
                        })
                    .toList())
            .build(),
        geometry.getCrs());
  }

  @Override
  default Geometry<?> visit(MultiSurface geometry) {
    return MultiSurface.of(
        ImmutableList.<Surface<?>>builder()
            .addAll(
                geometry.getValue().stream()
                    .map(mayThrow(geom -> geom.accept(this)))
                    .filter(Surface.class::isInstance)
                    .map(
                        geom -> {
                          if (geom instanceof CurvePolygon) {
                            return (CurvePolygon) geom;
                          } else if (geom instanceof PolyhedralSurface) {
                            return (PolyhedralSurface) geom;
                          }
                          return (Polygon) geom;
                        })
                    .toList())
            .build(),
        geometry.getCrs());
  }

  @Override
  default Geometry<?> visit(PolyhedralSurface geometry) {
    return PolyhedralSurface.of(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .map(Polygon.class::cast)
            .toList(),
        geometry.isClosed(),
        geometry.getCrs());
  }
}
