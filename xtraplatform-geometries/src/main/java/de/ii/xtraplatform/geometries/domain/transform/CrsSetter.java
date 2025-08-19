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
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.CircularString;
import de.ii.xtraplatform.geometries.domain.CompoundCurve;
import de.ii.xtraplatform.geometries.domain.Curve;
import de.ii.xtraplatform.geometries.domain.CurvePolygon;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryCollection;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.geometries.domain.ImmutableCircularString;
import de.ii.xtraplatform.geometries.domain.ImmutableCompoundCurve;
import de.ii.xtraplatform.geometries.domain.ImmutableCurvePolygon;
import de.ii.xtraplatform.geometries.domain.ImmutableGeometryCollection;
import de.ii.xtraplatform.geometries.domain.ImmutableLineString;
import de.ii.xtraplatform.geometries.domain.ImmutableMultiCurve;
import de.ii.xtraplatform.geometries.domain.ImmutableMultiLineString;
import de.ii.xtraplatform.geometries.domain.ImmutableMultiPoint;
import de.ii.xtraplatform.geometries.domain.ImmutableMultiPolygon;
import de.ii.xtraplatform.geometries.domain.ImmutableMultiSurface;
import de.ii.xtraplatform.geometries.domain.ImmutablePoint;
import de.ii.xtraplatform.geometries.domain.ImmutablePolygon;
import de.ii.xtraplatform.geometries.domain.ImmutablePolyhedralSurface;
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
import java.util.Optional;

public class CrsSetter implements GeometryTransformer {

  private final Optional<EpsgCrs> crs;

  public CrsSetter(Optional<EpsgCrs> crs) {
    this.crs = crs;
  }

  @Override
  public Geometry<?> visit(Point geometry) {
    return ImmutablePoint.copyOf(geometry).withCrs(crs);
  }

  @Override
  public Geometry<?> visit(SingleCurve geometry) {
    if (geometry.getType() == GeometryType.CIRCULAR_STRING) {
      return ImmutableCircularString.copyOf((CircularString) geometry).withCrs(crs);
    }
    return ImmutableLineString.copyOf((LineString) geometry).withCrs(crs);
  }

  @Override
  public Geometry<?> visit(MultiPoint geometry) {
    return ImmutableMultiPoint.builder()
        .crs(crs)
        .addAllValue(
            geometry.getValue().stream()
                .map(mayThrow(geom -> geom.accept(this)))
                .map(Point.class::cast)
                .toList())
        .build();
  }

  @Override
  public Geometry<?> visit(MultiLineString geometry) {
    return ImmutableMultiLineString.builder()
        .crs(crs)
        .addAllValue(
            geometry.getValue().stream()
                .map(mayThrow(geom -> geom.accept(this)))
                .map(LineString.class::cast)
                .toList())
        .build();
  }

  @Override
  public Geometry<?> visit(Polygon geometry) {
    return ImmutablePolygon.builder()
        .crs(crs)
        .addAllValue(
            geometry.getValue().stream()
                .map(mayThrow(geom -> geom.accept(this)))
                .map(LineString.class::cast)
                .toList())
        .build();
  }

  @Override
  public Geometry<?> visit(MultiPolygon geometry) {
    return ImmutableMultiPolygon.builder()
        .crs(crs)
        .addAllValue(
            geometry.getValue().stream()
                .map(mayThrow(geom -> geom.accept(this)))
                .map(Polygon.class::cast)
                .toList())
        .build();
  }

  @Override
  public Geometry<?> visit(CompoundCurve geometry) {
    return ImmutableCompoundCurve.builder()
        .crs(crs)
        .addAllValue(
            geometry.getValue().stream()
                .map(mayThrow(geom -> geom.accept(this)))
                .map(SingleCurve.class::cast)
                .toList())
        .build();
  }

  @Override
  public Geometry<?> visit(CurvePolygon geometry) {
    ImmutableList.Builder<Curve<?>> builder = ImmutableList.builder();
    geometry.getValue().stream()
        .map(mayThrow(geom -> geom.accept(this)))
        .forEach(geom -> builder.add((Curve<?>) geom));
    return ImmutableCurvePolygon.builder().crs(crs).addAllValue(builder.build()).build();
  }

  @Override
  public Geometry<?> visit(MultiCurve geometry) {
    ImmutableList.Builder<Curve<?>> builder = ImmutableList.builder();
    geometry.getValue().stream()
        .map(mayThrow(geom -> geom.accept(this)))
        .forEach(geom -> builder.add((Curve<?>) geom));
    return ImmutableMultiCurve.builder().crs(crs).addAllValue(builder.build()).build();
  }

  @Override
  public Geometry<?> visit(MultiSurface geometry) {
    ImmutableList.Builder<Surface<?>> builder = ImmutableList.builder();
    geometry.getValue().stream()
        .map(mayThrow(geom -> geom.accept(this)))
        .forEach(geom -> builder.add((Surface<?>) geom));
    return ImmutableMultiSurface.builder().crs(crs).addAllValue(builder.build()).build();
  }

  @Override
  public Geometry<?> visit(PolyhedralSurface geometry) {
    return ImmutablePolyhedralSurface.builder()
        .crs(crs)
        .addAllValue(
            geometry.getValue().stream()
                .map(mayThrow(geom -> geom.accept(this)))
                .map(Polygon.class::cast)
                .toList())
        .build();
  }

  @Override
  public Geometry<?> visit(GeometryCollection geometry) {
    ImmutableList.Builder<Geometry<?>> builder = ImmutableList.builder();
    geometry.getValue().stream().map(mayThrow(geom -> geom.accept(this))).forEach(builder::add);
    return ImmutableGeometryCollection.builder().crs(crs).addAllValue(builder.build()).build();
  }
}
