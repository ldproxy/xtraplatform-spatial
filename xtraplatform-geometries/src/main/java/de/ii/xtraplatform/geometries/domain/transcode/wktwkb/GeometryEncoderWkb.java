/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transcode.wktwkb;

import de.ii.xtraplatform.geometries.domain.AbstractGeometryCollection;
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
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;

public class GeometryEncoderWkb {

  static final byte[] ENDIANNESS =
      ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN ? new byte[] {0} : new byte[] {1};

  public GeometryEncoderWkb() {}

  public byte[] encode(Geometry<?> geometry) throws IOException {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream(1024);
    writeGeometry(stream, geometry, true);
    return stream.toByteArray();
  }

  private void writeGeometry(ByteArrayOutputStream stream, Geometry<?> geometry, boolean withHeader)
      throws IOException {
    if (withHeader) {
      stream.writeBytes(ENDIANNESS);
      writeUint32(
          stream,
          WktWkbGeometryType.fromGeometryType(geometry.getType()).toWkbCode(geometry.getAxes()));
    }

    switch (geometry.getType()) {
      case POINT -> writePoint(stream, (Point) geometry);
      case MULTI_POINT -> writeGeometryCollection(stream, (MultiPoint) geometry);
      case LINE_STRING -> writeSingleCurve(stream, (LineString) geometry);
      case MULTI_LINE_STRING -> writeGeometryCollection(stream, (MultiLineString) geometry);
      case POLYGON -> writePolygon(stream, (Polygon) geometry);
      case MULTI_POLYGON -> writeGeometryCollection(stream, (MultiPolygon) geometry);
      case CIRCULAR_STRING -> writeSingleCurve(stream, (CircularString) geometry);

      case COMPOUND_CURVE -> writeCompoundCurve(stream, (CompoundCurve) geometry);
      case CURVE_POLYGON -> writeCurvePolygon(stream, (CurvePolygon) geometry);
      case MULTI_CURVE -> writeGeometryCollection(stream, (MultiCurve) geometry);
      case MULTI_SURFACE -> writeGeometryCollection(stream, (MultiSurface) geometry);
      case GEOMETRY_COLLECTION -> writeGeometryCollection(stream, (GeometryCollection) geometry);
      case POLYHEDRAL_SURFACE -> writePolyhedralSurface(stream, (PolyhedralSurface) geometry);
    }
  }

  private void writeUint32(ByteArrayOutputStream stream, int uint32) {
    if (uint32 < 0) {
      throw new IllegalArgumentException("Unsigned Integer must be positive: " + uint32);
    }
    int value = uint32;
    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
      value = Integer.reverseBytes(value);
    }
    stream.write(value >> 24 & 0xFF);
    stream.write(value >> 16 & 0xFF);
    stream.write(value >> 8 & 0xFF);
    stream.write(value & 0xFF);
  }

  public void writeCoordinatesWkb(ByteArrayOutputStream stream, double[] coordinates)
      throws IOException {
    for (double coordinate : coordinates) {
      writeDouble(stream, coordinate);
    }
  }

  private void writeDouble(ByteArrayOutputStream stream, double value) throws IOException {
    long longBits = Double.doubleToRawLongBits(value);
    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
      longBits = Long.reverseBytes(longBits);
    }
    for (int i = 7; i >= 0; i--) {
      stream.write((int) (longBits >> (i * 8)) & 0xFF);
    }
  }

  private void writePoint(ByteArrayOutputStream stream, Point geometry) throws IOException {
    writeCoordinatesWkb(stream, geometry.getValue().getCoordinates());
  }

  private void writeSingleCurve(ByteArrayOutputStream stream, SingleCurve geometry)
      throws IOException {
    writeUint32(stream, geometry.getValue().getNumPositions());
    writeCoordinatesWkb(stream, geometry.getValue().getCoordinates());
  }

  private void writePolygon(ByteArrayOutputStream stream, Polygon geometry) throws IOException {
    writeUint32(stream, geometry.getValue().size());
    for (LineString ring : geometry.getValue()) {
      writeGeometry(stream, ring, false);
    }
  }

  private void writeGeometryCollection(
      ByteArrayOutputStream stream, AbstractGeometryCollection<?> geometry) throws IOException {
    writeUint32(stream, geometry.getValue().size());
    for (Geometry<?> geometry1 : geometry.getValue()) {
      writeGeometry(stream, geometry1, true);
    }
  }

  private void writeCompoundCurve(ByteArrayOutputStream stream, CompoundCurve geometry)
      throws IOException {
    writeUint32(stream, geometry.getValue().size());
    for (SingleCurve curve : geometry.getValue()) {
      writeGeometry(stream, curve, true);
    }
  }

  private void writeCurvePolygon(ByteArrayOutputStream stream, CurvePolygon geometry)
      throws IOException {
    writeUint32(stream, geometry.getValue().size());
    for (Curve<?> curve : geometry.getValue()) {
      writeGeometry(stream, curve, true);
    }
  }

  private void writePolyhedralSurface(ByteArrayOutputStream stream, PolyhedralSurface geometry)
      throws IOException {
    writeUint32(stream, geometry.getValue().size());
    for (Polygon polygon : geometry.getValue()) {
      writeGeometry(stream, polygon, true);
    }
  }
}
