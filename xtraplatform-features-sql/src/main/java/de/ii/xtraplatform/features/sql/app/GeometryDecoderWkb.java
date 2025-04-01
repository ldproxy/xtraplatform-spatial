/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.domain.FeatureEventHandler;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;

public class GeometryDecoderWkb {

  private static final long WKB25D = 0x80000000L;

  private final FeatureEventHandler<
          FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      handler;
  private final ModifiableContext<FeatureSchema, SchemaMapping> context;

  public GeometryDecoderWkb(
      FeatureEventHandler<
              FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
          handler,
      ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.handler = handler;
    this.context = context;
  }

  public void decode(byte[] wkb) throws IOException {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(wkb))) {
      byte byteOrder = dis.readByte();
      boolean isLittleEndian = (byteOrder == 1);

      // Reset the stream to re-read the data
      dis.reset();
      dis.readByte(); // Skip the byteOrder byte again

      long geometryTypeCode = readUnsignedInt(dis, isLittleEndian);
      boolean hasZ = (geometryTypeCode & WKB25D) != 0;
      geometryTypeCode &= ~WKB25D;

      SimpleFeatureGeometry geometryTypeEnum =
          SimpleFeatureGeometryFromToWkb.fromWkbType((int) geometryTypeCode)
              .toSimpleFeatureGeometry();
      int dimension = hasZ ? 3 : 2;

      if (!geometryTypeEnum.isValid()) {
        System.out.println("Invalid geometry type: " + geometryTypeEnum);
        return;
      }

      context.setGeometryType(geometryTypeEnum);
      context.setGeometryDimension(dimension);
      handler.onObjectStart(context);
      context.setInGeometry(true);

      switch (geometryTypeEnum) {
        case POINT:
          handlePointAndGetCoordinates(dis, isLittleEndian, dimension);
          break;
        case MULTI_POINT:
          handleMultiPoint(dis, isLittleEndian, dimension);
          break;
        case LINE_STRING:
          handleLineStringAndGetCoordinates(dis, isLittleEndian, dimension);
          break;
        case MULTI_LINE_STRING:
          handleMultiLineString(dis, isLittleEndian, dimension);
          break;
        case POLYGON:
          handlePolygon(dis, isLittleEndian, dimension);
          break;
        case MULTI_POLYGON:
          handleMultiPolygon(dis, isLittleEndian, dimension);
          break;
        case GEOMETRY_COLLECTION:
          break;
        default:
          throw new IOException("Unsupported geometry type: " + geometryTypeEnum);
      }

      context.setInGeometry(false);
      handler.onObjectEnd(context);
      context.setGeometryType(Optional.empty());
      context.setGeometryDimension(OptionalInt.empty());

    } catch (Exception e) {
      System.out.println("Failed to decode WKB: " + e.getMessage());
      throw new IOException("Failed to decode WKB", e);
    }
  }

  private void handlePointAndGetCoordinates(
      DataInputStream dis, boolean isLittleEndian, int dimension) throws IOException {
    String coordinates = readCoordinates(dis, isLittleEndian, dimension);
    context.setValueType(Type.STRING);
    context.setValue(coordinates);
    handler.onValue(context);
  }

  private void handleMultiPoint(DataInputStream dis, boolean isLittleEndian, int dimension)
      throws IOException {
    int numPoints = readInt(dis, isLittleEndian);
    handler.onArrayStart(context);
    for (int i = 0; i < numPoints; i++) {
      dis.readByte(); // byte order
      readInt(dis, isLittleEndian); // geometry type
      handlePointAndGetCoordinates(dis, isLittleEndian, dimension);
    }
    handler.onArrayEnd(context);
  }

  private String handleLineStringAndGetCoordinates(
      DataInputStream dis, boolean isLittleEndian, int dimension) throws IOException {
    int numPoints = readInt(dis, isLittleEndian);
    StringBuilder lineStringCoordinates = new StringBuilder(" ");
    for (int i = 0; i < numPoints; i++) {
      if (i > 0) {
        lineStringCoordinates.append(",");
      }
      String coordinates = readCoordinates(dis, isLittleEndian, dimension);
      lineStringCoordinates.append(coordinates);
    }
    context.setValueType(Type.STRING);
    context.setValue(lineStringCoordinates.toString());
    handler.onValue(context);
    return lineStringCoordinates.toString();
  }

  private void handleMultiLineString(DataInputStream dis, boolean isLittleEndian, int dimension)
      throws IOException {
    int numLineStrings = readInt(dis, isLittleEndian);
    if (numLineStrings < 0
        || numLineStrings > 1000) { // Add a reasonable upper limit for validation
      throw new IOException("Invalid number of lines: " + numLineStrings);
    }

    handler.onArrayStart(context);
    for (int i = 0; i < numLineStrings; i++) {
      dis.readByte(); // byte order
      readInt(dis, isLittleEndian); // geometry type
      handleLineStringAndGetCoordinates(dis, isLittleEndian, dimension);
    }
    handler.onArrayEnd(context);
  }

  private void handlePolygon(DataInputStream dis, boolean isLittleEndian, int dimension)
      throws IOException {
    int numRings = readInt(dis, isLittleEndian);
    handler.onArrayStart(context);
    if (numRings < 0 || numRings > 1000) { // Add a reasonable upper limit for validation
      throw new IOException("Invalid number of rings: " + numRings);
    }
    for (int i = 0; i < numRings; i++) {
      handleLineStringAndGetCoordinates(dis, isLittleEndian, dimension);
    }
    handler.onArrayEnd(context);
  }

  private void handleMultiPolygon(DataInputStream dis, boolean isLittleEndian, int dimension)
      throws IOException {
    int numPolygons = readInt(dis, isLittleEndian);
    handler.onArrayStart(context);
    for (int i = 0; i < numPolygons; i++) {
      dis.readByte(); // byte order
      int geometryType = readInt(dis, isLittleEndian);
      if (geometryType != getWkbType(SimpleFeatureGeometry.POLYGON)) {
        System.err.println("Invalid polygon geometry type: " + geometryType);
        continue; // Skip the invalid polygon
      }
      handlePolygon(dis, isLittleEndian, dimension);
    }
    handler.onArrayEnd(context);
  }

  private int readInt(DataInputStream dis, boolean isLittleEndian) throws IOException {
    int value = dis.readInt();
    int result = isLittleEndian ? Integer.reverseBytes(value) : value;
    return result;
  }

  private long readUnsignedInt(DataInputStream dis, boolean isLittleEndian) throws IOException {
    int value = dis.readInt();
    if (isLittleEndian) {
      value = Integer.reverseBytes(value);
    }
    long result = value & 0xFFFFFFFFL;
    return result;
  }

  private double readDouble(DataInputStream dis, boolean isLittleEndian) throws IOException {
    long value = dis.readLong();
    double result = Double.longBitsToDouble(isLittleEndian ? Long.reverseBytes(value) : value);
    return result;
  }

  private String readCoordinates(DataInputStream dis, boolean isLittleEndian, int dimension)
      throws IOException {
    double x = readDouble(dis, isLittleEndian);
    double y = readDouble(dis, isLittleEndian);
    Double z = null;
    if (dimension == 3) {
      z = readDouble(dis, isLittleEndian);
    }
    return (z != null)
        ? String.format(Locale.US, "%.3f %.3f %.3f", x, y, z)
        : String.format(Locale.US, "%.3f %.3f", x, y);
  }

  private int getWkbType(SimpleFeatureGeometry geometry) {
    switch (geometry) {
      case POINT:
        return 1;
      case LINE_STRING:
        return 2;
      case POLYGON:
        return 3;
      case MULTI_POINT:
        return 4;
      case MULTI_LINE_STRING:
        return 5;
      case MULTI_POLYGON:
        return 6;
      case GEOMETRY_COLLECTION:
        return 7;
      default:
        return -1;
    }
  }
}
