/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transcode.wktwkb;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.transcode.AbstractGeometryDecoder;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class GeometryDecoderWkb extends AbstractGeometryDecoder {

  private final boolean isOracle;

  public GeometryDecoderWkb() {
    this.isOracle = false;
  }

  // Oracle WKB differs in two aspects from standard WKB:
  // 1) CIRCULARSTRING, COMPOUNDCURVE, CURVEPOLYGON, MULTICURVE, and MULTISURFACE have a different
  // geometry type code.
  // 2) The embedded geometries do not repeat the endian byte for COMPOUNDCURVE, CURVEPOLYGON,
  // MULTICURVE, MULTISURFACE.
  public GeometryDecoderWkb(boolean isOracle) {
    this.isOracle = isOracle;
  }

  public Geometry<?> decode(byte[] wkb) throws IOException {
    return decode(wkb, Optional.empty());
  }

  public Geometry<?> decode(byte[] wkb, Optional<EpsgCrs> crs) throws IOException {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(wkb))) {
      return decode(dis, crs, Set.of(), null, Optional.empty());
    }
  }

  public Geometry<?> decode(
      DataInputStream dis,
      Optional<EpsgCrs> crs,
      Set<GeometryType> allowedTypes,
      Axes allowedAxes,
      Optional<Boolean> isLittleEndianOracle)
      throws IOException {
    boolean isLittleEndian =
        isOracle && isLittleEndianOracle.isPresent()
            ? isLittleEndianOracle.get()
            : dis.readByte() == 1;
    long typeCode = readUnsignedInt(dis, isLittleEndian);
    Axes axes = Axes.fromWkbCode(typeCode);
    GeometryType type = WktWkbGeometryType.fromWkbType((int) typeCode).toGeometryType();

    if (!allowedTypes.isEmpty() && !allowedTypes.contains(type)) {
      throw new IOException("Unexpected geometry type " + type + ". Allowed: " + allowedTypes);
    }
    if (allowedAxes != null && !allowedAxes.equals(axes)) {
      throw new IOException("Geometry axes " + axes + " do not match expected " + allowedAxes);
    }

    return switch (type) {
      case POINT -> point(readPosition(dis, isLittleEndian, axes), crs);
      case MULTI_POINT -> multiPoint2(
          readListOfGeometry(dis, crs, axes, isLittleEndian, Set.of(GeometryType.POINT)), crs);
      case LINE_STRING -> lineString(readPositionList(dis, isLittleEndian, axes), crs);
      case MULTI_LINE_STRING -> multiLineString2(
          readListOfGeometry(dis, crs, axes, isLittleEndian, Set.of(GeometryType.LINE_STRING)),
          crs);
      case POLYGON -> polygon(readListOfPositionList(dis, isLittleEndian, axes), crs);
      case MULTI_POLYGON -> multiPolygon2(
          readListOfGeometry(dis, crs, axes, isLittleEndian, Set.of(GeometryType.POLYGON)), crs);
      case CIRCULAR_STRING -> circularString(readPositionList(dis, isLittleEndian, axes), crs);
      case POLYHEDRAL_SURFACE -> polyhedralSurface2(
          readListOfGeometry(dis, crs, axes, isLittleEndian, Set.of(GeometryType.POLYGON)), crs);
      case COMPOUND_CURVE -> compoundCurve(
          readListOfGeometry(
              dis,
              crs,
              axes,
              isLittleEndian,
              Set.of(GeometryType.LINE_STRING, GeometryType.CIRCULAR_STRING)),
          crs);
      case CURVE_POLYGON -> curvePolygon(
          readListOfGeometry(
              dis,
              crs,
              axes,
              isLittleEndian,
              Set.of(
                  GeometryType.LINE_STRING,
                  GeometryType.CIRCULAR_STRING,
                  GeometryType.COMPOUND_CURVE)),
          crs);
      case MULTI_CURVE -> multiCurve(
          readListOfGeometry(
              dis,
              crs,
              axes,
              isLittleEndian,
              Set.of(
                  GeometryType.LINE_STRING,
                  GeometryType.CIRCULAR_STRING,
                  GeometryType.COMPOUND_CURVE)),
          crs);
      case MULTI_SURFACE -> multiSurface(
          readListOfGeometry(
              dis,
              crs,
              axes,
              isLittleEndian,
              Set.of(GeometryType.POLYGON, GeometryType.CURVE_POLYGON)),
          crs);
      case GEOMETRY_COLLECTION -> geometryCollection(
          readListOfGeometry(
              dis,
              crs,
              axes,
              isLittleEndian,
              Set.of(
                  GeometryType.POINT,
                  GeometryType.LINE_STRING,
                  GeometryType.POLYGON,
                  GeometryType.MULTI_POINT,
                  GeometryType.MULTI_LINE_STRING,
                  GeometryType.MULTI_POLYGON,
                  GeometryType.GEOMETRY_COLLECTION)),
          crs);
      default -> throw new IllegalStateException("Unsupported geometry type: " + type);
    };
  }

  private Position pointPos(Axes axes, double[] coords) {
    return Position.of(axes, coords);
  }

  private PositionList posList(Axes axes, double[] coords) {
    return PositionList.of(axes, coords);
  }

  private List<PositionList> posListList(Axes axes, List<double[]> coords) {
    return coords.stream().map(c -> PositionList.of(axes, c)).toList();
  }

  private long readUnsignedInt(DataInputStream dis, boolean isLittleEndian) throws IOException {
    int v = dis.readInt();
    return (isLittleEndian ? Integer.reverseBytes(v) : v) & 0xFFFFFFFFL;
  }

  private double readDouble(DataInputStream dis, boolean isLittleEndian) throws IOException {
    long v = dis.readLong();
    return Double.longBitsToDouble(isLittleEndian ? Long.reverseBytes(v) : v);
  }

  private Position readPosition(DataInputStream dis, boolean isLittleEndian, Axes axes)
      throws IOException {
    double[] coords = new double[axes.size()];
    for (int j = 0; j < axes.size(); j++) coords[j] = readDouble(dis, isLittleEndian);
    return pointPos(axes, coords);
  }

  private PositionList readPositionList(DataInputStream dis, boolean isLittleEndian, Axes axes)
      throws IOException {
    int dim = axes.size();
    long num = readUnsignedInt(dis, isLittleEndian);
    double[] coords = new double[dim * (int) num];
    for (int i = 0; i < num; i++)
      for (int j = 0; j < dim; j++) coords[i * dim + j] = readDouble(dis, isLittleEndian);
    return posList(axes, coords);
  }

  private List<PositionList> readListOfPositionList(
      DataInputStream dis, boolean isLittleEndian, Axes axes) throws IOException {
    int dim = axes.size();
    long num = readUnsignedInt(dis, isLittleEndian);
    List<double[]> list = new ArrayList<>();
    for (int k = 0; k < num; k++) {
      long numPoints = readUnsignedInt(dis, isLittleEndian);
      double[] coords = new double[dim * (int) numPoints];
      for (int i = 0; i < numPoints; i++)
        for (int j = 0; j < dim; j++) coords[i * dim + j] = readDouble(dis, isLittleEndian);
      list.add(coords);
    }
    return posListList(axes, list);
  }

  private List<Geometry<?>> readListOfGeometry(
      DataInputStream dis,
      Optional<EpsgCrs> crs,
      Axes axes,
      boolean isLittleEndian,
      Set<GeometryType> allowedTypes)
      throws IOException {
    long num = readUnsignedInt(dis, isLittleEndian);
    ImmutableList.Builder<Geometry<?>> builder = ImmutableList.builder();
    for (int i = 0; i < num; i++) {
      Geometry<?> g =
          decode(
              dis,
              crs,
              allowedTypes,
              axes,
              isOracle && allowedTypes.size() > 1 ? Optional.of(isLittleEndian) : Optional.empty());
      if (g != null) builder.add(g);
    }
    return builder.build();
  }
}
