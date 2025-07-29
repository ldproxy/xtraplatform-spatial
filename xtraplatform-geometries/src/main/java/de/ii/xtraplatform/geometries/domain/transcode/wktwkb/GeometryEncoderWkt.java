/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transcode.wktwkb;

import de.ii.xtraplatform.geometries.domain.CircularString;
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
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.Surface;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

// TODO change to GeometryVisitor
public class GeometryEncoderWkt {

  private final int[] precision;

  public GeometryEncoderWkt() {
    this.precision = new int[] {};
  }

  public GeometryEncoderWkt(List<Integer> precision) {
    this.precision =
        precision.stream().anyMatch(v -> v > 0)
            ? precision.stream().mapToInt(v -> v).toArray()
            : null;
  }

  public String encode(Geometry<?> geometry) throws IOException {
    final StringBuilder builder = new StringBuilder();
    writeGeometry(builder, geometry, true);
    return builder.toString();
  }

  private void writeGeometry(StringBuilder builder, Geometry<?> geometry, boolean withType)
      throws IOException {
    if (withType) {
      builder
          .append(WktWkbGeometryType.fromGeometryType(geometry.getType()).name())
          .append(geometry.getAxes().getWktSuffix());
    }

    if (geometry.isEmpty()) {
      builder.append(" EMPTY");
    } else {
      builder.append("(");
      switch (geometry.getType()) {
        case POINT -> writePoint(builder, (Point) geometry);
        case MULTI_POINT -> writeMultiPoint(builder, (MultiPoint) geometry);
        case LINE_STRING -> writeLineString(builder, (LineString) geometry);
        case MULTI_LINE_STRING -> writeMultiLineString(builder, (MultiLineString) geometry);
        case POLYGON -> writePolygon(builder, (Polygon) geometry);
        case MULTI_POLYGON -> writeMultiPolygon(builder, (MultiPolygon) geometry);
        case CIRCULAR_STRING -> writeCircularString(builder, (CircularString) geometry);
        case COMPOUND_CURVE -> writeCompoundCurve(builder, (CompoundCurve) geometry);
        case CURVE_POLYGON -> writeCurvePolygon(builder, (CurvePolygon) geometry);
        case MULTI_CURVE -> writeMultiCurve(builder, (MultiCurve) geometry);
        case MULTI_SURFACE -> writeMultiSurface(builder, (MultiSurface) geometry);
        case GEOMETRY_COLLECTION -> writeGeometryCollection(builder, (GeometryCollection) geometry);
        case POLYHEDRAL_SURFACE -> writePolyhedralSurface(builder, (PolyhedralSurface) geometry);
      }
      builder.append(")");
    }
  }

  private void writePosition(StringBuilder builder, Position position, boolean inParentheses) {
    double[] coordinates = position.getCoordinates();
    int dimension = position.getAxes().size();
    if (inParentheses) {
      builder.append("(");
    }
    for (int i = 0; i < coordinates.length; i++) {
      int axisIndex = i % dimension;
      String value =
          precision.length > axisIndex && precision[axisIndex] > 0
              ? BigDecimal.valueOf(coordinates[i])
                  .setScale(precision[axisIndex], RoundingMode.HALF_UP)
                  .toPlainString()
              : String.valueOf(coordinates[i]);
      if (i > 0) {
        builder.append(" ");
      }
      builder.append(value);
      if (inParentheses) {
        builder.append(")");
      }
    }
  }

  private void writePositionList(
      StringBuilder builder, PositionList positionList, boolean inParentheses) {
    double[] coordinates = positionList.getCoordinates();
    int dimension = positionList.getAxes().size();
    if (inParentheses) {
      builder.append("(");
    }
    for (int i = 0; i < coordinates.length; i++) {
      int axisIndex = i % dimension;
      String value =
          precision.length > axisIndex && precision[axisIndex] > 0
              ? BigDecimal.valueOf(coordinates[i])
                  .setScale(precision[axisIndex], RoundingMode.HALF_UP)
                  .toPlainString()
              : String.valueOf(coordinates[i]);
      if (i > 0) {
        if (axisIndex == 0) {
          builder.append(",");
        } else {
          builder.append(" ");
        }
      }
      builder.append(value);
      if (inParentheses) {
        builder.append(")");
      }
    }
  }

  private void writePoint(StringBuilder builder, Point geometry) {
    writePosition(builder, geometry.getValue(), false);
  }

  private void writeMultiPoint(StringBuilder builder, MultiPoint geometry) throws IOException {
    boolean first = true;
    for (Point point : geometry.getValue()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      builder.append("(");
      writePosition(builder, point.getValue(), false);
      builder.append(")");
    }
  }

  private void writeLineString(StringBuilder builder, LineString geometry) {
    writePositionList(builder, geometry.getValue(), false);
  }

  private void writeMultiLineString(StringBuilder builder, MultiLineString geometry)
      throws IOException {
    boolean first = true;
    for (LineString lineString : geometry.getValue()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      writeGeometry(builder, lineString, false);
    }
  }

  private void writePolygon(StringBuilder builder, Polygon geometry) {
    for (LineString ring : geometry.getValue()) {
      if (ring.isEmpty()) {
        builder.append("EMPTY");
      } else {
        builder.append("(");
        writePositionList(builder, ring.getValue(), false);
        builder.append(")");
      }
    }
  }

  private void writeMultiPolygon(StringBuilder builder, MultiPolygon geometry) throws IOException {
    boolean first = true;
    for (Polygon polygon : geometry.getValue()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      writeGeometry(builder, polygon, false);
    }
  }

  private void writeCircularString(StringBuilder builder, CircularString geometry) {
    writePositionList(builder, geometry.getValue(), false);
  }

  private void writeCompoundCurve(StringBuilder builder, CompoundCurve geometry)
      throws IOException {
    boolean first = true;
    for (Curve<?> curve : geometry.getValue()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      writeGeometry(builder, curve, !(curve instanceof LineString));
    }
  }

  private void writeCurvePolygon(StringBuilder builder, CurvePolygon geometry) throws IOException {
    boolean first = true;
    for (Curve<?> curve : geometry.getValue()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      writeGeometry(builder, curve, !(curve instanceof LineString));
    }
  }

  private void writePolyhedralSurface(StringBuilder builder, PolyhedralSurface geometry)
      throws IOException {
    boolean first = true;
    for (Polygon polygon : geometry.getValue()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      writeGeometry(builder, polygon, false);
    }
  }

  private void writeGeometryCollection(StringBuilder builder, GeometryCollection geometry)
      throws IOException {
    boolean first = true;
    for (Geometry<?> geometry1 : geometry.getValue()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      writeGeometry(builder, geometry1, true);
    }
  }

  private void writeMultiSurface(StringBuilder builder, MultiSurface geometry) throws IOException {
    boolean first = true;
    for (Surface<?> surface : geometry.getValue()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      writeGeometry(builder, surface, !(surface instanceof Polygon));
    }
  }

  private void writeMultiCurve(StringBuilder builder, MultiCurve geometry) throws IOException {
    boolean first = true;
    for (Curve<?> curve : geometry.getValue()) {
      if (!first) {
        builder.append(",");
      }
      first = false;
      writeGeometry(builder, curve, !(curve instanceof LineString));
    }
  }
}
