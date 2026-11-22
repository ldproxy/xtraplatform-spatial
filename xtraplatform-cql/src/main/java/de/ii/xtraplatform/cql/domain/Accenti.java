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
@JsonDeserialize(builder = ImmutableAccenti.Builder.class)
@JsonSerialize(using = Accenti.Serializer.class)
@FunctionalInterface
public interface Accenti extends Scalar {

  String TYPE = "accenti";

  @JsonProperty(TYPE)
  Operand getValue();

  static Accenti of(Operand value) {
    return new ImmutableAccenti.Builder().value(value).build();
  }

  @Override
  default <U> U accept(CqlVisitor<U> visitor) {

    U value = getValue().accept(visitor);

    return visitor.visit(
        this, Objects.nonNull(value) ? ImmutableList.of(value) : ImmutableList.of());
  }

  class Serializer extends StdSerializer<Accenti> {

    private static final long serialVersionUID = 1L;

    protected Serializer() {
      super(Accenti.class);
    }

    @Override
    public void serialize(Accenti value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      CqlJsonOperations.write(gen, TYPE, List.of(value.getValue()));
    }
  }
}
