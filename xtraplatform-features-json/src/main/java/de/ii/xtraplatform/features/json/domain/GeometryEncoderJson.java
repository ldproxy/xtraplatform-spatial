/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.domain;

import com.fasterxml.jackson.core.JsonGenerator;
import de.ii.xtraplatform.geometries.domain.AbstractGeometryCollection;
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
import de.ii.xtraplatform.geometries.domain.transform.GeometryVisitor;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class GeometryEncoderJson implements GeometryVisitor<Void> {

  private final JsonGenerator json;
  private final boolean onlyGeoJsonGeometries;
  private final int[] precision;

  public GeometryEncoderJson(JsonGenerator json) {
    this.json = json;
    this.onlyGeoJsonGeometries = false;
    this.precision = null;
  }

  public GeometryEncoderJson(
      JsonGenerator json, boolean onlyGeoJsonGeometries, List<Integer> precision) {
    this.json = json;
    this.onlyGeoJsonGeometries = onlyGeoJsonGeometries;
    this.precision =
        precision.stream().anyMatch(v -> v > 0)
            ? precision.stream().mapToInt(v -> v).toArray()
            : null;
  }

  @Override
  public Void visit(Point geometry) {
    writeJson(
        json -> {
          json.writeStartObject();
          json.writeStringField("type", "Point");
          json.writeFieldName("coordinates");
          writePosition(geometry.getValue().getCoordinates());
          json.writeEndObject();
        });
    return null;
  }

  @Override
  public Void visit(MultiPoint geometry) {
    writeJson(
        json -> {
          json.writeStartObject();
          json.writeStringField("type", "MultiPoint");
          json.writeFieldName("coordinates");
          json.writeStartArray();
          for (Point point : geometry.getValue()) {
            writePosition(point.getValue().getCoordinates());
          }
          json.writeEndArray();
          json.writeEndObject();
        });
    return null;
  }

  @Override
  public Void visit(SingleCurve geometry) {
    writeJson(
        json -> {
          json.writeStartObject();
          json.writeStringField(
              "type",
              switch (geometry.getType()) {
                case LINE_STRING -> "LineString";
                case CIRCULAR_STRING -> "CircularString";
                default -> throw new IllegalArgumentException(
                    "Unsupported geometry type: " + geometry.getType());
              });
          json.writeFieldName("coordinates");
          writePositionList(geometry.getAxes().size(), geometry.getValue().getCoordinates());
          json.writeEndObject();
        });
    return null;
  }

  @Override
  public Void visit(MultiLineString geometry) {
    writeJson(
        json -> {
          json.writeStartObject();
          json.writeStringField("type", "MultiLineString");
          json.writeFieldName("coordinates");
          json.writeStartArray();
          for (LineString lineString : geometry.getValue()) {
            writePositionList(geometry.getAxes().size(), lineString.getValue().getCoordinates());
          }
          json.writeEndArray();
          json.writeEndObject();
        });
    return null;
  }

  @Override
  public Void visit(Polygon geometry) {
    writeJson(
        json -> {
          json.writeStartObject();
          json.writeStringField("type", "Polygon");
          json.writeFieldName("coordinates");
          json.writeStartArray();
          for (LineString ring : geometry.getValue()) {
            writePositionList(geometry.getAxes().size(), ring.getValue().getCoordinates());
          }
          json.writeEndArray();
          json.writeEndObject();
        });
    return null;
  }

  @Override
  public Void visit(MultiPolygon geometry) {
    writeJson(
        json -> {
          json.writeStartObject();
          json.writeStringField("type", "MultiPolygon");
          json.writeFieldName("coordinates");
          json.writeStartArray();
          for (Polygon polygon : geometry.getValue()) {
            json.writeStartArray();
            for (LineString ring : polygon.getValue()) {
              writePositionList(geometry.getAxes().size(), ring.getValue().getCoordinates());
            }
            json.writeEndArray();
          }
          json.writeEndArray();
          json.writeEndObject();
        });
    return null;
  }

  @Override
  public Void visit(GeometryCollection geometry) {
    writeAbstractGeometryCollection(geometry);
    return null;
  }

  @Override
  public Void visit(CompoundCurve geometry) {
    if (onlyGeoJsonGeometries) {
      throw new IllegalStateException("Cannot write CompoundCurve geometry in GeoJSON.");
    }
    writeJson(
        json -> {
          json.writeStartObject();
          json.writeStringField("type", "CompoundCurve");
          json.writeFieldName("geometries");
          json.writeStartArray();
          for (SingleCurve curve : geometry.getValue()) {
            curve.accept(this);
          }
          json.writeEndArray();
          json.writeEndObject();
        });
    return null;
  }

  @Override
  public Void visit(CurvePolygon geometry) {
    if (onlyGeoJsonGeometries) {
      throw new IllegalStateException("Cannot write CurvePolygon geometry in GeoJSON.");
    }
    writeJson(
        json -> {
          json.writeStartObject();
          json.writeStringField("type", "CurvePolygon");
          json.writeFieldName("geometries");
          json.writeStartArray();
          for (Curve<?> curve : geometry.getValue()) {
            curve.accept(this);
          }
          json.writeEndArray();
          json.writeEndObject();
        });
    return null;
  }

  @Override
  public Void visit(MultiCurve geometry) {
    writeAbstractGeometryCollection(geometry);
    return null;
  }

  @Override
  public Void visit(MultiSurface geometry) {
    writeAbstractGeometryCollection(geometry);
    return null;
  }

  @Override
  public Void visit(PolyhedralSurface geometry) {
    writeJson(
        json -> {
          if (geometry.isClosed() && !onlyGeoJsonGeometries) {
            json.writeStartObject();
            json.writeStringField("type", "Polyhedron");
            json.writeFieldName("coordinates");
            json.writeStartArray();
            json.writeStartArray();
            for (Polygon polygon : geometry.getValue()) {
              json.writeStartArray();
              for (LineString ring : polygon.getValue()) {
                writePositionList(geometry.getAxes().size(), ring.getValue().getCoordinates());
              }
              json.writeEndArray();
            }
            json.writeEndArray();
            json.writeEndArray();
            json.writeEndObject();
          } else {
            json.writeStartObject();
            json.writeStringField("type", "MultiPolygon");
            json.writeFieldName("coordinates");
            json.writeStartArray();
            for (Polygon polygon : geometry.getValue()) {
              json.writeStartArray();
              for (LineString ring : polygon.getValue()) {
                writePositionList(geometry.getAxes().size(), ring.getValue().getCoordinates());
              }
              json.writeEndArray();
            }
            json.writeEndArray();
            json.writeEndObject();
          }
        });
    return null;
  }

  private void writePosition(double[] coordinates) {
    writeJson(
        json -> {
          if (Double.isNaN(coordinates[0])) {
            json.writeStartArray();
            json.writeEndArray();
          } else if (precision != null) {
            json.writeStartArray();
            for (int i = 0; i < coordinates.length; i++) {
              if (i < precision.length && precision[i] > 0) {
                json.writeNumber(
                    BigDecimal.valueOf(coordinates[i])
                        .setScale(precision[i], RoundingMode.HALF_UP));
              } else {
                json.writeNumber(coordinates[i]);
              }
            }
            json.writeEndArray();
          } else {
            json.writeArray(coordinates, 0, coordinates.length);
          }
        });
  }

  private void writePositionList(int dimension, double[] coordinates) {
    writeJson(
        json -> {
          json.writeStartArray();
          for (int i = 0; i < coordinates.length / dimension; i++) {
            if (Double.isNaN(coordinates[i * dimension])) {
              continue;
            }
            if (precision != null) {
              json.writeStartArray();
              for (int j = 0; j < dimension; j++) {
                if (j < precision.length && precision[j] > 0) {
                  json.writeNumber(
                      BigDecimal.valueOf(coordinates[i * dimension + j])
                          .setScale(precision[j], RoundingMode.HALF_UP));
                } else {
                  json.writeNumber(coordinates[i * dimension + j]);
                }
              }
              json.writeEndArray();
            } else {
              json.writeArray(coordinates, i * dimension, dimension);
            }
          }
          json.writeEndArray();
        });
  }

  private void writeAbstractGeometryCollection(AbstractGeometryCollection<?> geometry) {
    writeJson(
        json -> {
          json.writeStartObject();
          json.writeStringField(
              "type",
              switch (geometry.getType()) {
                case GEOMETRY_COLLECTION -> "GeometryCollection";
                case MULTI_CURVE -> "MultiCurve";
                case MULTI_SURFACE -> "MultiSurface";
                default -> throw new IllegalArgumentException(
                    "Unsupported geometry type: " + geometry.getType());
              });
          json.writeFieldName("geometries");
          json.writeStartArray();
          for (Geometry<?> geometry1 : geometry.getValue()) {
            geometry1.accept(this);
          }
          json.writeEndArray();
          json.writeEndObject();
        });
  }

  private void writeJson(IOConsumer<JsonGenerator> writer) {
    try {
      writer.accept(json);
    } catch (IOException e) {
      throw new IllegalStateException("Error while writing a JSON geometry.", e);
    }
  }

  @FunctionalInterface
  public interface IOConsumer<T> {
    void accept(T t) throws IOException;
  }
}
