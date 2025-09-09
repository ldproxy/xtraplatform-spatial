/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transcode.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.transcode.AbstractGeometryDecoder;
import de.ii.xtraplatform.geometries.domain.transform.CrsSetter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GeometryDecoderJson extends AbstractGeometryDecoder {

  private final boolean geoJsonOnly;

  public GeometryDecoderJson() {
    this.geoJsonOnly = false;
  }

  public GeometryDecoderJson(boolean geoJsonOnly) {
    this.geoJsonOnly = geoJsonOnly;
  }

  public Geometry<?> decode(JsonParser parser, Optional<EpsgCrs> crs, Optional<Axes> axes)
      throws IOException {
    String type = null;
    Object coordinatesRaw = null;
    boolean hasMeasures = false;
    List<Geometry<?>> geometries = List.of();
    JsonToken token = parser.currentToken();
    Optional<EpsgCrs> coordRefSys = crs;
    if (token == null) {
      token = parser.nextToken();
    }
    if (token != JsonToken.START_OBJECT) {
      throw new IOException("Expected START_OBJECT token, but got: " + token);
    }

    while (!parser.isClosed() && token != JsonToken.END_OBJECT) {
      token = parser.nextToken();
      if (token == JsonToken.FIELD_NAME) {
        String name = parser.currentName();
        if ("type".equals(name)) {
          parser.nextToken();
          type = parser.getText();
        } else if ("measures".equals(name)) {
          token = parser.nextToken();
          if (token != JsonToken.START_OBJECT) {
            throw new IOException("Expected START_OBJECT token, but got: " + token);
          }
          while (token != JsonToken.END_OBJECT) {
            token = parser.nextToken();
            if (token == JsonToken.FIELD_NAME && "enabled".equals(parser.currentName())) {
              token = parser.nextToken();
              if (token != JsonToken.VALUE_TRUE && token != JsonToken.VALUE_FALSE) {
                throw new IOException("Expected boolean value for 'enabled', but got: " + token);
              }
              hasMeasures = parser.getBooleanValue();
            }
          }
          if (hasMeasures) {
            // update geometries that have already been parsed
          }
        } else if ("coordRefSys".equals(name)) {
          coordRefSys = parseCoordRefSys(parser);
          // update geometries that have already been parsed
          CrsSetter crsSetter = new CrsSetter(coordRefSys);
          ImmutableList.Builder<Geometry<?>> builder = ImmutableList.builder();
          geometries.forEach(geom -> builder.add(geom.accept(crsSetter)));
          geometries = builder.build();
        } else if ("coordinates".equals(name)) {
          token = parser.nextToken();
          if (token != JsonToken.START_ARRAY) {
            throw new IOException("Expected START_ARRAY token, but got: " + token);
          }
          coordinatesRaw = readCoordinatesRaw(parser);
          token = parser.currentToken();
          if (token != JsonToken.END_ARRAY) {
            throw new IOException("Expected END_ARRAY token, but got: " + token);
          }
        } else if ("geometries".equals(name)) {
          token = parser.nextToken();
          if (token != JsonToken.START_ARRAY) {
            throw new IOException("Expected START_ARRAY token, but got: " + token);
          }
          geometries = readGeometries(parser, coordRefSys, axes);
          token = parser.currentToken();
          if (token != JsonToken.END_OBJECT && token != JsonToken.END_ARRAY) {
            throw new IOException("Expected END_ARRAY or END_OBJECT token, but got: " + token);
          }
        } else {
          // Unrecognized field, skip it
          parser.skipChildren();
        }
      }
    }

    GeometryType geometryType =
        geoJsonOnly
            ? GeoJsonGeometryType.forString(type).toGeometryType()
            : JsonFgGeometryType.forString(type).toGeometryType();

    // check if the required fields are present
    switch (geometryType) {
      case POINT,
          MULTI_POINT,
          LINE_STRING,
          MULTI_LINE_STRING,
          POLYGON,
          MULTI_POLYGON,
          CIRCULAR_STRING,
          POLYHEDRAL_SURFACE -> Objects.requireNonNull(
          coordinatesRaw, "Coordinates must not be null for type: " + type);
      case COMPOUND_CURVE, CURVE_POLYGON, MULTI_CURVE, MULTI_SURFACE, GEOMETRY_COLLECTION -> Objects
          .requireNonNull(geometries, "Geometries must not be null for type: " + type);
    }

    return switch (geometryType) {
      case POINT -> point(toPosition(axes, coordinatesRaw), coordRefSys);
      case MULTI_POINT -> multiPoint(toListOfPositions(axes, coordinatesRaw), coordRefSys);
      case LINE_STRING -> lineString(toPositionList(axes, coordinatesRaw), coordRefSys);
      case MULTI_LINE_STRING -> multiLineString(
          toListOfPositionList(axes, coordinatesRaw), coordRefSys);
      case POLYGON -> polygon(toListOfPositionList(axes, coordinatesRaw), coordRefSys);
      case MULTI_POLYGON -> multiPolygon(
          toListOfListOfPositionList(axes, coordinatesRaw), coordRefSys);
      case CIRCULAR_STRING -> circularString(toPositionList(axes, coordinatesRaw), coordRefSys);
      case POLYHEDRAL_SURFACE -> polyhedralSurface(
          toListOfListOfListOfPositionList(axes, coordinatesRaw).get(0), true, coordRefSys);
      case COMPOUND_CURVE -> compoundCurve(geometries, coordRefSys);
      case CURVE_POLYGON -> curvePolygon(geometries, coordRefSys);
      case MULTI_CURVE -> multiCurve(geometries, coordRefSys);
      case MULTI_SURFACE -> multiSurface(geometries, coordRefSys);
      case GEOMETRY_COLLECTION -> geometryCollection(geometries, coordRefSys);
      default -> throw new IllegalStateException("Unsupported geometry type: " + type);
    };
  }

  public Geometry<?> decode(JsonNode node, Optional<EpsgCrs> crs, Optional<Axes> axes)
      throws IOException {
    if (!node.isObject()) {
      throw new IOException(
          "Expected OBJECT representing a geometry, but got: " + node.getNodeType());
    }

    String type = node.get("type").asText();
    GeometryType geometryType =
        geoJsonOnly
            ? GeoJsonGeometryType.forString(type).toGeometryType()
            : JsonFgGeometryType.forString(type).toGeometryType();

    Optional<EpsgCrs> coordRefSys = crs;
    boolean hasMeasures = false;
    if (node.has("coordRefSys")) {
      coordRefSys = parseCoordRefSys(node);
    }
    if (node.has("measures")) {
      hasMeasures = node.get("measures").get("enabled").asBoolean();
    }

    return switch (geometryType) {
      case POINT,
          MULTI_POINT,
          LINE_STRING,
          MULTI_LINE_STRING,
          POLYGON,
          MULTI_POLYGON,
          CIRCULAR_STRING,
          POLYHEDRAL_SURFACE -> {
        if (!node.has("coordinates")) {
          throw new IOException("Coordinates must not be null for type: " + type);
        }
        Object coordinatesRaw = readCoordinatesRaw((ArrayNode) node.get("coordinates"));

        yield switch (geometryType) {
          case POINT -> point(toPosition(axes, coordinatesRaw), coordRefSys);
          case MULTI_POINT -> multiPoint(toListOfPositions(axes, coordinatesRaw), coordRefSys);
          case LINE_STRING -> lineString(toPositionList(axes, coordinatesRaw), coordRefSys);
          case MULTI_LINE_STRING -> multiLineString(
              toListOfPositionList(axes, coordinatesRaw), coordRefSys);
          case POLYGON -> polygon(toListOfPositionList(axes, coordinatesRaw), coordRefSys);
          case MULTI_POLYGON -> multiPolygon(
              toListOfListOfPositionList(axes, coordinatesRaw), coordRefSys);
          case CIRCULAR_STRING -> circularString(toPositionList(axes, coordinatesRaw), coordRefSys);
          case POLYHEDRAL_SURFACE -> polyhedralSurface(
              toListOfListOfListOfPositionList(axes, coordinatesRaw).get(0), true, coordRefSys);
          default -> throw new IllegalStateException("Unsupported geometry type: " + type);
        };
      }
      case COMPOUND_CURVE, CURVE_POLYGON, MULTI_CURVE, MULTI_SURFACE, GEOMETRY_COLLECTION -> {
        if (!node.has("geometries")) {
          throw new IOException("Geometries must not be null for type: " + type);
        }
        ImmutableList.Builder<Geometry<?>> builder = ImmutableList.builder();
        for (JsonNode geom : node.get("geometries")) {
          builder.add(decode(geom, coordRefSys, axes));
        }
        List<Geometry<?>> geometries = builder.build();
        yield switch (geometryType) {
          case COMPOUND_CURVE -> compoundCurve(geometries, coordRefSys);
          case CURVE_POLYGON -> curvePolygon(geometries, coordRefSys);
          case MULTI_CURVE -> multiCurve(geometries, coordRefSys);
          case MULTI_SURFACE -> multiSurface(geometries, coordRefSys);
          case GEOMETRY_COLLECTION -> geometryCollection(geometries, coordRefSys);
          default -> throw new IllegalStateException("Unsupported geometry type: " + type);
        };
      }
      default -> throw new IllegalStateException("Unsupported geometry type: " + type);
    };
  }

  private Position toPosition(Optional<Axes> axes, Object coordinatesRaw) {
    double[] coords = toDoubleArray(coordinatesRaw);
    Axes finalAxes = getAxes(axes, coords);
    return Position.of(finalAxes, coords);
  }

  private PositionList toPositionList(Optional<Axes> axes, Object coordinatesRaw) {
    List<double[]> coords = toListOfDoubleArray(coordinatesRaw);
    return coords.isEmpty()
        ? PositionList.empty(axes.orElse(Axes.XY))
        : PositionList.of(getAxes(axes, coords.get(0)), flatten(coords));
  }

  private List<Position> toListOfPositions(Optional<Axes> axes, Object coordinatesRaw) {
    return toListOfDoubleArray(coordinatesRaw).stream()
        .map(c -> Position.of(getAxes(axes, c), c))
        .toList();
  }

  private List<PositionList> toListOfPositionList(Optional<Axes> axes, Object coordinatesRaw) {
    return toListOfListOfDoubleArray(coordinatesRaw).stream()
        .map(
            c ->
                c.isEmpty()
                    ? PositionList.empty(axes.orElse(Axes.XY))
                    : PositionList.of(getAxes(axes, c.get(0)), flatten(c)))
        .toList();
  }

  private List<List<PositionList>> toListOfListOfPositionList(
      Optional<Axes> axes, Object coordinatesRaw) {
    return toListOfListOfListOfDoubleArray(coordinatesRaw).stream()
        .map(
            coord ->
                coord.stream()
                    .map(
                        c ->
                            c.isEmpty()
                                ? PositionList.empty(axes.orElse(Axes.XY))
                                : PositionList.of(getAxes(axes, c.get(0)), flatten(c)))
                    .toList())
        .toList();
  }

  private List<List<List<PositionList>>> toListOfListOfListOfPositionList(
      Optional<Axes> axes, Object coordinatesRaw) {
    return toListOfListOfListOfListOfDoubleArray(coordinatesRaw).stream()
        .map(
            coords ->
                coords.stream()
                    .map(
                        coord ->
                            coord.stream()
                                .map(
                                    c ->
                                        c.isEmpty()
                                            ? PositionList.empty(axes.orElse(Axes.XY))
                                            : PositionList.of(getAxes(axes, c.get(0)), flatten(c)))
                                .toList())
                    .toList())
        .toList();
  }

  private static Axes getAxes(Optional<Axes> axes, double[] coords) {
    Axes finalAxes;
    if (axes.isEmpty()) {
      if (coords.length == 2) {
        finalAxes = Axes.XY;
      } else if (coords.length == 3) {
        finalAxes = Axes.XYZ;
      } else if (coords.length == 4) {
        finalAxes = Axes.XYZ;
      } else {
        throw new IllegalStateException("Invalid coordinates for Point: " + coords.length);
      }
    } else {
      finalAxes = axes.get();
    }
    return finalAxes;
  }

  private List<Geometry<?>> readGeometries(
      JsonParser parser, Optional<EpsgCrs> crs, Optional<Axes> axes) throws IOException {
    List<Geometry<?>> geometries = new ArrayList<>();
    while (parser.nextToken() == JsonToken.START_OBJECT) {
      geometries.add(decode(parser, crs, axes));
    }
    return geometries;
  }

  private Object readCoordinatesRaw(JsonParser parser) throws IOException {
    return readArray(parser);
  }

  private Object readArray(JsonParser parser) throws IOException {
    ImmutableList.Builder<Object> list = ImmutableList.builder();
    while (parser.nextToken() != JsonToken.END_ARRAY) {
      if (parser.currentToken() == JsonToken.START_ARRAY) {
        list.add(readArray(parser));
      } else if (parser.currentToken().isNumeric()) {
        list.add(parser.getDoubleValue());
      } else {
        throw new IllegalStateException("Unexpected Token: " + parser.currentToken());
      }
    }
    return list.build();
  }

  private Object readCoordinatesRaw(ArrayNode arrayNode) throws IOException {
    return readArray(arrayNode);
  }

  private Object readArray(ArrayNode arrayNode) throws IOException {
    ImmutableList.Builder<Object> list = ImmutableList.builder();
    for (JsonNode node : arrayNode) {
      if (node.isArray()) {
        list.add(readArray((ArrayNode) node));
      } else if (node.isNumber()) {
        list.add(node.asDouble());
      } else {
        throw new IllegalStateException("Unexpected Node Type: " + node.getNodeType());
      }
    }
    return list.build();
  }

  private double[] toDoubleArray(Object obj) {
    List<?> list = (List<?>) obj;
    double[] arr = new double[list.size()];
    for (int i = 0; i < list.size(); i++) arr[i] = ((Double) list.get(i)).doubleValue();
    return arr;
  }

  private List<double[]> toListOfDoubleArray(Object obj) {
    List<?> outer = (List<?>) obj;
    ImmutableList.Builder<double[]> result = ImmutableList.builder();
    for (Object inner : outer) result.add(toDoubleArray(inner));
    return result.build();
  }

  private List<List<double[]>> toListOfListOfDoubleArray(Object obj) {
    List<?> outer = (List<?>) obj;
    ImmutableList.Builder<List<double[]>> result = ImmutableList.builder();
    for (Object inner : outer) result.add(toListOfDoubleArray(inner));
    return result.build();
  }

  private List<List<List<double[]>>> toListOfListOfListOfDoubleArray(Object obj) {
    List<?> outer = (List<?>) obj;
    ImmutableList.Builder<List<List<double[]>>> result = ImmutableList.builder();
    for (Object inner : outer) result.add(toListOfListOfDoubleArray(inner));
    return result.build();
  }

  private List<List<List<List<double[]>>>> toListOfListOfListOfListOfDoubleArray(Object obj) {
    List<?> outer = (List<?>) obj;
    ImmutableList.Builder<List<List<List<double[]>>>> result = ImmutableList.builder();
    for (Object inner : outer) result.add(toListOfListOfListOfDoubleArray(inner));
    return result.build();
  }

  private double[] flatten(List<double[]> coords) {
    List<Double> flat = new ArrayList<>();
    for (double[] arr : coords) {
      for (double d : arr) flat.add(d);
    }
    return flat.stream().mapToDouble(Double::doubleValue).toArray();
  }

  private Optional<EpsgCrs> parseCoordRefSys(JsonParser parser) throws IOException {
    JsonToken token = parser.nextToken();
    if (token == JsonToken.VALUE_STRING) {
      return Optional.of(EpsgCrs.fromString(parser.getText()));
    } else if (token == JsonToken.START_OBJECT) {
      Optional<EpsgCrs> crs = Optional.empty();
      while (token != JsonToken.END_OBJECT) {
        token = parser.nextToken();
        if (token == JsonToken.FIELD_NAME && "href".equals(parser.currentName())) {
          token = parser.nextToken();
          if (token != JsonToken.VALUE_STRING) {
            throw new IOException("Expected string value for 'href', but got: " + token);
          }
          crs = Optional.of(EpsgCrs.fromString(parser.getText()));
        }
      }
      return crs;
    } else if (token == JsonToken.START_ARRAY) {
      Optional<EpsgCrs> crs = Optional.empty();
      while (token != JsonToken.END_ARRAY) {
        Optional<EpsgCrs> crs2 = parseCoordRefSys(parser);
        if (crs2.isPresent()) {
          crs =
              crs.map(
                      epsgCrs ->
                          EpsgCrs.of(
                              epsgCrs.getCode(), crs2.get().getCode(), epsgCrs.getForceAxisOrder()))
                  .or(() -> crs2);
        }
        token = parser.currentToken();
      }
      return crs;
    } else {
      throw new IOException(
          "Expected STRING, START_OBJECT or START_ARRAY token for 'coordRefSys', but got: "
              + token);
    }
  }

  private Optional<EpsgCrs> parseCoordRefSys(JsonNode node) throws IOException {
    if (node.isTextual()) {
      return Optional.of(EpsgCrs.fromString(node.asText()));
    } else if (node.isObject() && node.has("href")) {
      return parseCoordRefSys(node.get("href"));
    } else if (node.isArray()) {
      Optional<EpsgCrs> crs = Optional.empty();
      for (JsonNode n : ((ArrayNode) node)) {
        Optional<EpsgCrs> crs2 = parseCoordRefSys(n);
        if (crs2.isPresent()) {
          crs =
              crs.map(
                      epsgCrs ->
                          EpsgCrs.of(
                              epsgCrs.getCode(), crs2.get().getCode(), epsgCrs.getForceAxisOrder()))
                  .or(() -> crs2);
        }
      }
      return crs;
    } else {
      throw new IOException(
          "Expected STRING, OBJECT or ARRAY for 'coordRefSys', but got: " + node.getNodeType());
    }
  }
}
