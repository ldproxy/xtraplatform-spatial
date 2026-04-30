/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableArrayLiteral.Builder.class)
public interface ArrayLiteral extends Vector, Literal, Scalar {

  @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
  static ArrayLiteral of(String elements) throws CqlParseException {
    return new ArrayLiteral.Builder(elements).build();
  }

  static ArrayLiteral of(List<Scalar> elements) {
    return new ArrayLiteral.Builder(elements).build();
  }

  class Builder extends ImmutableArrayLiteral.Builder {
    public Builder() {
      super();
    }

    @JsonCreator
    public Builder(List<Scalar> literal) {
      super();
      value(literal);
      type(List.class);
    }

    @JsonCreator
    @SuppressWarnings("PMD.AvoidUncheckedExceptionsInSignatures")
    public Builder(String literal) throws CqlParseException {
      super();
      value(literal);
      type(String.class);
    }
  }
}
