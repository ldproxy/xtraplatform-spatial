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
import com.fasterxml.jackson.annotation.OptBoolean;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Preconditions;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(using = Bbox.Serializer.class)
@JsonDeserialize(builder = ImmutableBbox.Builder.class)
public interface Bbox extends CqlNode {

  @JsonProperty("bbox")
  List<Double> getCoordinates();

  @JsonIgnore
  @JacksonInject(value = "filterCrs", optional = OptBoolean.TRUE)
  Optional<EpsgCrs> getCrs();

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

  @Value.Check
  default void check() {
    Preconditions.checkArgument(
        getCrs().isPresent()
            || getCrs().filter(crs -> crs.equals(OgcCrs.CRS84)).isEmpty()
            || (getCoordinates().get(0) >= -180.0 && getCoordinates().get(0) <= 180.0),
        "longitude (%s) must be between -180 and 180 degrees",
        getCoordinates().get(0));
    Preconditions.checkArgument(
        getCrs().isPresent()
            || getCrs().filter(crs -> crs.equals(OgcCrs.CRS84)).isEmpty()
            || (getCoordinates().get(1) >= -90.0 && getCoordinates().get(1) <= 90.0),
        "latitude (%s) must be less between -90 and 90 degrees",
        getCoordinates().get(1));
    Preconditions.checkArgument(
        getCrs().isPresent()
            || getCrs().filter(crs -> crs.equals(OgcCrs.CRS84)).isEmpty()
            || (getCoordinates().get(2) >= -180.0 && getCoordinates().get(2) <= 180.0),
        "longitude (%s) must be less between -180 and 180 degrees",
        getCoordinates().get(2));
    Preconditions.checkArgument(
        getCrs().isPresent()
            || getCrs().filter(crs -> crs.equals(OgcCrs.CRS84)).isEmpty()
            || (getCoordinates().get(3) >= -90.0 && getCoordinates().get(3) <= 90.0),
        "latitude (%s) must be less between -90 and 90 degrees",
        getCoordinates().get(3));
  }

  class Serializer extends StdSerializer<de.ii.xtraplatform.cql.domain.Bbox> {

    protected Serializer() {
      this(null);
    }

    protected Serializer(Class<de.ii.xtraplatform.cql.domain.Bbox> t) {
      super(t);
    }

    @Override
    public void serialize(
        de.ii.xtraplatform.cql.domain.Bbox geom,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeFieldName("bbox");
      jsonGenerator.writeStartArray();
      for (Double ord : geom.getCoordinates()) {
        jsonGenerator.writeNumber(ord);
      }
      jsonGenerator.writeEndArray();
      jsonGenerator.writeEndObject();
    }
  }
}
