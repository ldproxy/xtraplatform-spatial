/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transcode;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.CircularString;
import de.ii.xtraplatform.geometries.domain.CompoundCurve;
import de.ii.xtraplatform.geometries.domain.Curve;
import de.ii.xtraplatform.geometries.domain.CurvePolygon;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryCollection;
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
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import de.ii.xtraplatform.geometries.domain.Surface;
import java.util.List;
import java.util.Optional;

public abstract class AbstractGeometryDecoder {

  protected Geometry<?> empty(GeometryType type, Axes axes) {
    return switch (type) {
      case POINT -> Point.empty(axes);
      case MULTI_POINT -> MultiPoint.empty(axes);
      case LINE_STRING -> LineString.empty(axes);
      case MULTI_LINE_STRING -> MultiLineString.empty(axes);
      case POLYGON -> Polygon.empty(axes);
      case MULTI_POLYGON -> MultiPolygon.empty(axes);
      case CIRCULAR_STRING -> CircularString.empty(axes);
      case COMPOUND_CURVE -> CompoundCurve.empty(axes);
      case CURVE_POLYGON -> CurvePolygon.empty(axes);
      case MULTI_CURVE -> MultiCurve.empty(axes);
      case POLYHEDRAL_SURFACE -> PolyhedralSurface.empty(axes);
      case MULTI_SURFACE -> MultiSurface.empty(axes);
      case GEOMETRY_COLLECTION -> GeometryCollection.empty(axes);
      default -> throw new IllegalStateException("Unsupported geometry type: " + type);
    };
  }

  protected Geometry<?> point(Position pos, Optional<EpsgCrs> crs) {
    return Point.of(pos, crs);
  }

  protected Geometry<?> lineString(PositionList posList, Optional<EpsgCrs> crs) {
    return LineString.of(posList, crs);
  }

  protected Geometry<?> circularString(PositionList posList, Optional<EpsgCrs> crs) {
    return CircularString.of(posList, crs);
  }

  protected Geometry<?> polygon(List<PositionList> rings, Optional<EpsgCrs> crs) {
    return Polygon.of(rings, crs);
  }

  protected Geometry<?> multiPoint(List<Position> points, Optional<EpsgCrs> crs) {
    return MultiPoint.of(points.stream().map(p -> Point.of(p, crs)).toList(), crs);
  }

  protected Geometry<?> multiLineString(List<PositionList> lists, Optional<EpsgCrs> crs) {
    return MultiLineString.of(lists.stream().map(l -> LineString.of(l, crs)).toList(), crs);
  }

  protected Geometry<?> multiPolygon(List<List<PositionList>> polygons, Optional<EpsgCrs> crs) {
    return MultiPolygon.of(polygons.stream().map(r -> Polygon.of(r, crs)).toList(), crs);
  }

  protected Geometry<?> polyhedralSurface(
      List<List<PositionList>> polygons, boolean closed, Optional<EpsgCrs> crs) {
    return PolyhedralSurface.of(
        polygons.stream().map(r -> Polygon.of(r, crs)).toList(), closed, crs);
  }

  protected Geometry<?> polyhedralSurface(
      List<List<PositionList>> polygons, Optional<EpsgCrs> crs) {
    return PolyhedralSurface.of(
        polygons.stream().map(r -> Polygon.of(r, crs)).toList(), false, crs);
  }

  protected Geometry<?> multiPoint2(List<Geometry<?>> points, Optional<EpsgCrs> crs) {
    return MultiPoint.of(
        points.stream().filter(Point.class::isInstance).map(Point.class::cast).toList(), crs);
  }

  protected Geometry<?> multiLineString2(List<Geometry<?>> lineStrings, Optional<EpsgCrs> crs) {
    return MultiLineString.of(
        lineStrings.stream()
            .filter(LineString.class::isInstance)
            .map(LineString.class::cast)
            .toList(),
        crs);
  }

  protected Geometry<?> multiPolygon2(List<Geometry<?>> polygons, Optional<EpsgCrs> crs) {
    return MultiPolygon.of(
        polygons.stream().filter(Polygon.class::isInstance).map(Polygon.class::cast).toList(), crs);
  }

  protected Geometry<?> polyhedralSurface2(List<Geometry<?>> polygons, Optional<EpsgCrs> crs) {
    return PolyhedralSurface.of(
        polygons.stream().filter(Polygon.class::isInstance).map(Polygon.class::cast).toList(),
        false,
        crs);
  }

  protected Geometry<?> compoundCurve(List<Geometry<?>> curves, Optional<EpsgCrs> crs) {
    return CompoundCurve.of(
        curves.stream().filter(SingleCurve.class::isInstance).map(SingleCurve.class::cast).toList(),
        crs);
  }

  protected Geometry<?> curvePolygon(List<Geometry<?>> rings, Optional<EpsgCrs> crs) {
    return CurvePolygon.of(
        ImmutableList.<Curve<?>>builder()
            .addAll(
                rings.stream()
                    .filter(Curve.class::isInstance)
                    .map(
                        geom ->
                            geom instanceof CompoundCurve
                                ? (CompoundCurve) geom
                                : (SingleCurve) geom)
                    .toList())
            .build(),
        crs);
  }

  protected Geometry<?> multiCurve(List<Geometry<?>> curves, Optional<EpsgCrs> crs) {
    return MultiCurve.of(
        ImmutableList.<Curve<?>>builder()
            .addAll(
                curves.stream()
                    .filter(Curve.class::isInstance)
                    .map(
                        geom ->
                            geom instanceof CompoundCurve
                                ? (CompoundCurve) geom
                                : (SingleCurve) geom)
                    .toList())
            .build(),
        crs);
  }

  protected Geometry<?> multiSurface(List<Geometry<?>> surfaces, Optional<EpsgCrs> crs) {
    return MultiSurface.of(
        ImmutableList.<Surface<?>>builder()
            .addAll(
                surfaces.stream()
                    .filter(Surface.class::isInstance)
                    .map(
                        geom ->
                            geom instanceof CurvePolygon
                                ? (CurvePolygon) geom
                                : geom instanceof PolyhedralSurface
                                    ? (PolyhedralSurface) geom
                                    : (Polygon) geom)
                    .toList())
            .build(),
        crs);
  }

  protected Geometry<?> geometryCollection(List<Geometry<?>> geoms, Optional<EpsgCrs> crs) {
    return GeometryCollection.of(ImmutableList.<Geometry<?>>builder().addAll(geoms).build(), crs);
  }
}
