/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

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
public interface Function extends Temporal, Cql2Expression {

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

  class Serializer extends StdSerializer<Function> {

    private static final long serialVersionUID = 1L;

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
