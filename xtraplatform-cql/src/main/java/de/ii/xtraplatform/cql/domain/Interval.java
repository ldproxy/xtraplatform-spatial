/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableInterval.Builder.class)
@JsonSerialize(using = Interval.Serializer.class)
@FunctionalInterface
public interface Interval extends Temporal {

  @JsonProperty("interval")
  List<Operand> getArgs();

  static Interval of(List<Operand> arguments) {
    return new ImmutableInterval.Builder().args(arguments).build();
  }

  @Override
  default <U> U accept(CqlVisitor<U> visitor) {

    List<U> arguments =
        getArgs().stream().map(argument -> argument.accept(visitor)).collect(Collectors.toList());

    return visitor.visit(this, arguments);
  }

  class Serializer extends StdSerializer<Interval> {

    private static final long serialVersionUID = 1L;

    protected Serializer() {
      this(null);
    }

    protected Serializer(Class<Interval> t) {
      super(t);
    }

    @Override
    public void serialize(Interval value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeFieldName("interval");
      gen.writeStartArray();
      for (Operand operand : value.getArgs()) {
        if (operand instanceof TemporalLiteral) {
          gen.writeString(String.format("%s", ((TemporalLiteral) operand).getValue().toString()));
        } else {
          gen.writeObject(operand);
        }
      }
      gen.writeEndArray();
      gen.writeEndObject();
    }
  }
}
