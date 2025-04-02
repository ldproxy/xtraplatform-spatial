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

      dis.reset();
      dis.readByte();

      long geometryTypeCode = readUnsignedInt(dis, isLittleEndian);

      boolean hasZ = (geometryTypeCode > 1000);

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
          handleLineStringAndGetCoordinates(dis, isLittleEndian, dimension, 1);
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
      e.printStackTrace();
      throw new IOException("Failed to decode WKB", e);
    }
  }

  private void handlePointAndGetCoordinates(
      DataInputStream dis, boolean isLittleEndian, int dimension) throws IOException {
    String coordinates = readCoordinates(dis, isLittleEndian, dimension, 1);
    context.setValueType(Type.STRING);
    context.setValue(coordinates);
    handler.onValue(context);
  }

  private void handleMultiPoint(DataInputStream dis, boolean isLittleEndian, int dimension)
      throws IOException {
    int numPoints = readInt(dis, isLittleEndian);
    handler.onArrayStart(context);

    for (int i = 0; i < numPoints; i++) {
      dis.readByte();
      int geometryType = readInt(dis, isLittleEndian);
      if (geometryType != 1 && geometryType != 1001) {
        throw new IOException("Invalid point geometry type: " + geometryType);
      }

      double x = readDouble(dis, isLittleEndian);
      double y = readDouble(dis, isLittleEndian);
      StringBuilder point = new StringBuilder(formatCoordinate(x) + " " + formatCoordinate(y));

      if (dimension == 3 && geometryType == 1001) {
        double z = readDouble(dis, isLittleEndian);
        point.append(" ").append(formatCoordinate(z));
      }

      context.setValueType(Type.STRING);
      context.setValue(point.toString());
      handler.onValue(context);
    }

    handler.onArrayEnd(context);
  }

  private String handleLineStringAndGetCoordinates(
      DataInputStream dis, boolean isLittleEndian, int dimension, int numRings) throws IOException {
    int numPoints = readInt(dis, isLittleEndian);
    StringBuilder lineStringCoordinates = new StringBuilder("");

    String coordinates = readCoordinates(dis, isLittleEndian, dimension, numPoints);
    lineStringCoordinates.append(coordinates);

    context.setValueType(Type.STRING);
    context.setValue(lineStringCoordinates.toString());
    handler.onValue(context);
    return lineStringCoordinates.toString();
  }

  private void handleMultiLineString(DataInputStream dis, boolean isLittleEndian, int dimension)
      throws IOException {
    handler.onArrayStart(context);

    int numLineStrings = readInt(dis, isLittleEndian);
    if (numLineStrings < 0 || numLineStrings > 1000) {
      throw new IOException("Invalid number of lines: " + numLineStrings);
    }

    for (int i = 0; i < numLineStrings; i++) {
      dis.readByte();
      readInt(dis, isLittleEndian);
      handleLineStringAndGetCoordinates(dis, isLittleEndian, dimension, numLineStrings);
    }
    handler.onArrayEnd(context);
  }

  private void handlePolygon(DataInputStream dis, boolean isLittleEndian, int dimension)
      throws IOException {
    handler.onArrayStart(context);

    int numRings = readInt(dis, isLittleEndian);

    if (numRings < 0 || numRings > 1000) {
      throw new IOException("Invalid number of rings: " + numRings);
    }

    for (int i = 0; i < numRings; i++) {
      handleLineStringAndGetCoordinates(dis, isLittleEndian, dimension, numRings);
    }
    handler.onArrayEnd(context);
  }

  private void handleMultiPolygon(DataInputStream dis, boolean isLittleEndian, int dimension)
      throws IOException {
    handler.onArrayStart(context);

    int numPolygons = readInt(dis, isLittleEndian);
    for (int i = 0; i < numPolygons; i++) {
      dis.readByte();
      int geometryType = readInt(dis, isLittleEndian);
      if (geometryType != 3 && geometryType != 1003) {
        System.err.println("Invalid polygon geometry type: " + geometryType);
        continue;
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

  private String readCoordinates(
      DataInputStream dis, boolean isLittleEndian, int dimension, int numPoints)
      throws IOException {
    StringBuilder coordinates = new StringBuilder();

    for (int i = 0; i < numPoints; i++) {
      if (i > 0) {
        coordinates.append(",");
      }
      double x = readDouble(dis, isLittleEndian);
      double y = readDouble(dis, isLittleEndian);
      if (dimension == 3) {
        double z = readDouble(dis, isLittleEndian);
        coordinates
            .append(formatCoordinate(x))
            .append(" ")
            .append(formatCoordinate(y))
            .append(" ")
            .append(formatCoordinate(z));
      } else {
        coordinates.append(formatCoordinate(x)).append(" ").append(formatCoordinate(y));
      }
    }
    String result = coordinates.toString();
    return result;
  }

  private String formatCoordinate(Double value) {
    String formatted = String.format(Locale.US, "%.3f", value);
    String[] parts = formatted.split("\\.");
    if (parts.length > 1) {
      parts[1] = parts[1].replaceAll("0+$", ""); // Remove trailing zeros after the decimal point
      if (parts[1].isEmpty()) {
        return parts[0]; // If no digits remain after the decimal point, return the integer part
      }
      return parts[0] + "." + parts[1]; // Return the number with simplified decimal part
    }
    return formatted;
  }
}
