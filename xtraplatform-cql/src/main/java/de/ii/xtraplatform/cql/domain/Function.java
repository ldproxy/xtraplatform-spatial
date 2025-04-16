/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
@JsonDeserialize(builder = ImmutableFunction.Builder.class)
@JsonSerialize(using = Function.Serializer.class)
public interface Function extends CqlNode, Scalar, Temporal, Operand, Cql2Expression {

  String getName();

  List<Operand> getArgs();

  static Function of(String name, List<Operand> arguments) {
    return new ImmutableFunction.Builder().name(name).args(arguments).build();
  }

  @Override
  default <U> U accept(CqlVisitor<U> visitor) {

    List<U> arguments =
        getArgs().stream().map(argument -> argument.accept(visitor)).collect(Collectors.toList());

    return visitor.visit(this, arguments);
  }

  @JsonIgnore
  @Value.Lazy
  default Class<?> getType() {
    if (isLower() || isUpper()) {
      return String.class;
    } else if (isPosition()) {
      return Integer.class;
    } else if (isDiameter()) {
      return Double.class;
    } else if (isAlike()) {
      return Boolean.class;
    }
    return Object.class;
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isCasei() {
    return "casei".equalsIgnoreCase(getName());
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isAccenti() {
    return "accenti".equalsIgnoreCase(getName());
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isInterval() {
    return "interval".equalsIgnoreCase(getName());
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isLower() {
    return "lower".equalsIgnoreCase(getName());
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isUpper() {
    return "upper".equalsIgnoreCase(getName());
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isPosition() {
    return "position".equalsIgnoreCase(getName());
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isDiameter() {
    return "diameter2d".equalsIgnoreCase(getName()) || "diameter3d".equalsIgnoreCase(getName());
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isAlike() {
    return "alike".equalsIgnoreCase(getName());
  }

  class Serializer extends StdSerializer<Function> {

    protected Serializer() {
      this(null);
    }

    protected Serializer(Class<Function> t) {
      super(t);
    }

    @Override
    public void serialize(Function value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeFieldName("function");
      gen.writeStartObject();
      gen.writeStringField("name", value.getName());
      gen.writeFieldName("args");
      gen.writeStartArray();
      for (Operand operand : value.getArgs()) {
        if (operand instanceof ScalarLiteral) {
          gen.writeString(String.format("%s", ((ScalarLiteral) operand).getValue().toString()));
        } else {
          gen.writeObject(operand);
        }
      }
      gen.writeEndArray();
      gen.writeEndObject();
      gen.writeEndObject();
    }
  }
}
