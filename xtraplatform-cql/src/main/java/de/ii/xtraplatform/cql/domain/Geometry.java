/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Preconditions;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type",
    defaultImpl = Geometry.Bbox.class)
@JsonSubTypes({
  @JsonSubTypes.Type(value = Geometry.Point.class, name = "Point"),
  @JsonSubTypes.Type(value = Geometry.LineString.class, name = "LineString"),
  @JsonSubTypes.Type(value = Geometry.Polygon.class, name = "Polygon"),
  @JsonSubTypes.Type(value = Geometry.MultiPoint.class, name = "MultiPoint"),
  @JsonSubTypes.Type(value = Geometry.MultiLineString.class, name = "MultiLineString"),
  @JsonSubTypes.Type(value = Geometry.MultiPolygon.class, name = "MultiPolygon"),
  @JsonSubTypes.Type(value = Geometry.GeometryCollection.class, name = "GeometryCollection")
})
@JsonSerialize(using = Geometry.Serializer.class)
public interface Geometry<T> extends CqlNode {

  // TODO migrate to new Geometry interface

  enum Type {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
    BBox
  }

  Type getType();

  List<T> getCoordinates();

  @JsonIgnore
  @JacksonInject("filterCrs")
  Optional<EpsgCrs> getCrs();

  class Serializer extends StdSerializer<Geometry<?>> {

    protected Serializer() {
      this(null);
    }

    protected Serializer(Class<Geometry<?>> t) {
      super(t);
    }

    @Override
    public void serialize(
        Geometry<?> geom, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      if (geom instanceof Bbox) {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeFieldName("bbox");
        jsonGenerator.writeStartArray();
        for (Double ord : ((Bbox) geom).getCoordinates()) {
          jsonGenerator.writeNumber(ord);
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
      } else {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("type", geom.getType().toString());
        if (geom instanceof Point) {
          jsonGenerator.writeFieldName("coordinates");
          jsonGenerator.writeStartArray();
          for (Double ord : ((Point) geom).getCoordinates().get(0)) {
            jsonGenerator.writeNumber(ord);
          }
          jsonGenerator.writeEndArray();
        } else if (geom instanceof MultiPoint) {
          jsonGenerator.writeFieldName("coordinates");
          jsonGenerator.writeStartArray();
          for (Point p : ((MultiPoint) geom).getCoordinates()) {
            jsonGenerator.writeStartArray();
            for (Double ord : p.getCoordinates().get(0)) {
              jsonGenerator.writeNumber(ord);
            }
            jsonGenerator.writeEndArray();
          }
          jsonGenerator.writeEndArray();
        } else if (geom instanceof MultiLineString) {
          jsonGenerator.writeFieldName("coordinates");
          jsonGenerator.writeStartArray();
          for (LineString g : ((MultiLineString) geom).getCoordinates()) {
            jsonGenerator.writeStartArray();
            for (Coordinate c : g.getCoordinates()) {
              jsonGenerator.writeStartArray();
              for (Double ord : c) {
                jsonGenerator.writeNumber(ord);
              }
              jsonGenerator.writeEndArray();
            }
            jsonGenerator.writeEndArray();
          }
          jsonGenerator.writeEndArray();
        } else if (geom instanceof MultiPolygon) {
          jsonGenerator.writeFieldName("coordinates");
          jsonGenerator.writeStartArray();
          for (Polygon g : ((MultiPolygon) geom).getCoordinates()) {
            jsonGenerator.writeStartArray();
            for (List<Coordinate> r : g.getCoordinates()) {
              jsonGenerator.writeStartArray();
              for (Coordinate c : r) {
                jsonGenerator.writeStartArray();
                for (Double ord : c) {
                  jsonGenerator.writeNumber(ord);
                }
                jsonGenerator.writeEndArray();
              }
              jsonGenerator.writeEndArray();
            }
            jsonGenerator.writeEndArray();
          }
          jsonGenerator.writeEndArray();
        } else if (geom instanceof GeometryCollection) {
          jsonGenerator.writeFieldName("geometries");
          jsonGenerator.writeStartArray();
          for (Object p : ((GeometryCollection) geom).getCoordinates()) {
            this.serialize((Geometry<?>) p, jsonGenerator, serializerProvider);
          }
          jsonGenerator.writeEndArray();
        } else {
          serializerProvider.defaultSerializeField(
              "coordinates", geom.getCoordinates(), jsonGenerator);
        }
        jsonGenerator.writeEndObject();
      }
    }
  }

  @Value.Immutable
  @JsonDeserialize(using = Point.PointDeserializer.class)
  interface Point extends Geometry<Coordinate> {

    static Point of(double x, double y) {
      return new ImmutablePoint.Builder().addCoordinates(Coordinate.of(x, y)).build();
    }

    static Point of(double x, double y, EpsgCrs crs) {
      return new ImmutablePoint.Builder().addCoordinates(Coordinate.of(x, y)).crs(crs).build();
    }

    static Point of(Coordinate coordinate) {
      return new ImmutablePoint.Builder().addCoordinates(coordinate).build();
    }

    static Point of(EpsgCrs crs, Coordinate coordinate) {
      return new ImmutablePoint.Builder().crs(crs).addCoordinates(coordinate).build();
    }

    class PointDeserializer extends StdDeserializer<Point> {

      protected PointDeserializer() {
        this(null);
      }

      protected PointDeserializer(Class<?> vc) {
        super(vc);
      }

      @Override
      public Point deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
          throws IOException, JacksonException {
        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);
        return Point.of(oc.treeToValue(node.get("coordinates"), Coordinate.class));
      }
    }

    @Value.Check
    default void check() {
      Preconditions.checkState(
          getCoordinates().size() == 1,
          "a point must have only one coordinate",
          getCoordinates().size());
    }

    @Override
    default Type getType() {
      return Type.Point;
    }
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableLineString.Builder.class)
  interface LineString extends Geometry<Coordinate> {

    static LineString of(Coordinate... coordinates) {
      return new ImmutableLineString.Builder().addCoordinates(coordinates).build();
    }

    static LineString of(EpsgCrs crs, Coordinate... coordinates) {
      return new ImmutableLineString.Builder().crs(crs).addCoordinates(coordinates).build();
    }

    @Override
    default Type getType() {
      return Type.LineString;
    }
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutablePolygon.Builder.class)
  interface Polygon extends Geometry<List<Coordinate>> {

    static Polygon of(List<Coordinate>... coordinates) {
      return new ImmutablePolygon.Builder().addCoordinates(coordinates).build();
    }

    static Polygon of(EpsgCrs crs, List<Coordinate>... coordinates) {
      return new ImmutablePolygon.Builder().crs(crs).addCoordinates(coordinates).build();
    }

    @Override
    default Type getType() {
      return Type.Polygon;
    }
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableMultiPoint.Builder.class)
  interface MultiPoint extends Geometry<Point> {

    static MultiPoint of(Point... points) {
      return new ImmutableMultiPoint.Builder().addCoordinates(points).build();
    }

    static MultiPoint of(EpsgCrs crs, Point... points) {
      return new ImmutableMultiPoint.Builder().crs(crs).addCoordinates(points).build();
    }

    @Override
    default Type getType() {
      return Type.MultiPoint;
    }
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableMultiLineString.Builder.class)
  interface MultiLineString extends Geometry<LineString> {

    static MultiLineString of(LineString... lineStrings) {
      return new ImmutableMultiLineString.Builder().addCoordinates(lineStrings).build();
    }

    static MultiLineString of(EpsgCrs crs, LineString... lineStrings) {
      return new ImmutableMultiLineString.Builder().crs(crs).addCoordinates(lineStrings).build();
    }

    @Override
    default Type getType() {
      return Type.MultiLineString;
    }
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableMultiPolygon.Builder.class)
  interface MultiPolygon extends Geometry<Polygon> {

    static MultiPolygon of(Polygon... polygons) {
      return new ImmutableMultiPolygon.Builder().addCoordinates(polygons).build();
    }

    static MultiPolygon of(EpsgCrs crs, Polygon... polygons) {
      return new ImmutableMultiPolygon.Builder().crs(crs).addCoordinates(polygons).build();
    }

    @Override
    default Type getType() {
      return Type.MultiPolygon;
    }
  }

  @Value.Immutable
  @JsonDeserialize(using = Geometry.GeometryCollection.Deserializer.class)
  // Note that Immutables does not support Geometry<Geometry<?>>, so Object is used instead
  // and casts are added as needed
  interface GeometryCollection extends Geometry<Object> {

    static GeometryCollection of(Geometry<?>... geometries) {
      return new ImmutableGeometryCollection.Builder().addCoordinates(geometries).build();
    }

    @JsonProperty("geometries")
    @Override
    List<Object> getCoordinates();

    @Override
    default Type getType() {
      return Type.GeometryCollection;
    }

    class Deserializer extends StdDeserializer<GeometryCollection> {

      protected Deserializer() {
        this(null);
      }

      protected Deserializer(Class<GeometryCollection> t) {
        super(t);
      }

      @Override
      public GeometryCollection deserialize(
          JsonParser jsonParser, DeserializationContext deserializationContext)
          throws IOException, JacksonException {
        ImmutableGeometryCollection.Builder builder = new ImmutableGeometryCollection.Builder();
        ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        JsonNode geometries = node.get("geometries");
        if (geometries.isArray()) {
          for (JsonNode geomNode : geometries) {
            builder.addCoordinates(mapper.treeToValue(geomNode, Geometry.class));
          }
        }
        return builder.build();
      }
    }
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableBbox.Builder.class)
  interface Bbox extends Geometry<Double> {

    static Bbox of(BoundingBox boundingBox) {
      return new ImmutableBbox.Builder()
          .addCoordinates(
              boundingBox.getXmin(),
              boundingBox.getYmin(),
              boundingBox.getXmax(),
              boundingBox.getYmax())
          .crs(boundingBox.getEpsgCrs())
          .build();
    }

    static Bbox of(double xmin, double ymin, double xmax, double ymax) {
      return new ImmutableBbox.Builder().addCoordinates(xmin, ymin, xmax, ymax).build();
    }

    static Bbox of(double xmin, double ymin, double xmax, double ymax, EpsgCrs crs) {
      return new ImmutableBbox.Builder().addCoordinates(xmin, ymin, xmax, ymax).crs(crs).build();
    }

    @JsonIgnore
    @Override
    default Type getType() {
      return Type.BBox;
    }

    @JsonProperty("bbox")
    @Override
    List<Double> getCoordinates();
  }

  class Coordinate extends ArrayList<Double> implements CqlNode {

    public static Coordinate of(double x, double y) {
      return new Coordinate(x, y);
    }

    public static Coordinate of(double x, double y, double z) {
      return new Coordinate(x, y, z);
    }

    public Coordinate(Double x, Double y) {
      super();
      add(x);
      add(y);
    }

    public Coordinate(Double x, Double y, Double z) {
      this(x, y);
      add(z);
    }

    public Coordinate() {
      super();
    }
  }
}
