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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.transcode.json.GeometryEncoderJson;
import java.io.IOException;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(using = GeometryNode.Serializer.class)
public interface GeometryNode extends CqlNode {

  @Value.Parameter
  Geometry<?> getGeometry();

  @JsonIgnore
  @JacksonInject("filterCrs")
  Optional<EpsgCrs> getCrs();

  static GeometryNode of(Geometry<?> geometry) {
    return ImmutableGeometryNode.of(geometry);
  }

  class Serializer extends StdSerializer<GeometryNode> {

    protected Serializer() {
      this(null);
    }

    protected Serializer(Class<GeometryNode> t) {
      super(t);
    }

    @Override
    public void serialize(
        GeometryNode geom, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      geom.getGeometry().accept(new GeometryEncoderJson(jsonGenerator));
    }
  }
}
