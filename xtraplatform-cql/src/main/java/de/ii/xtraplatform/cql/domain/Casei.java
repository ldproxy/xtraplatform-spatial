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
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableCasei.Builder.class)
@JsonSerialize(using = Casei.Serializer.class)
@FunctionalInterface
public interface Casei extends Scalar {

  String TYPE = "casei";

  @JsonProperty(TYPE)
  Operand getValue();

  static Casei of(Operand value) {
    return new ImmutableCasei.Builder().value(value).build();
  }

  @Override
  default <U> U accept(CqlVisitor<U> visitor) {

    U value = getValue().accept(visitor);

    return visitor.visit(
        this, Objects.nonNull(value) ? ImmutableList.of(value) : ImmutableList.of());
  }

  class Serializer extends StdSerializer<Casei> {

    private static final long serialVersionUID = 1L;

    protected Serializer() {
      super(Casei.class);
    }

    @Override
    public void serialize(Casei value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      CqlJsonOperations.write(gen, TYPE, List.of(value.getValue()));
    }
  }
}
