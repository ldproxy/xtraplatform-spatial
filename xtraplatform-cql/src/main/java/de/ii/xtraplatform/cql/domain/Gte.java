/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableGte.Builder.class)
@FunctionalInterface
public interface Gte extends BinaryScalarOperation {

  String TYPE = ">=";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static Gte of(List<Scalar> operands) {
    return new ImmutableGte.Builder().args(operands).build();
  }

  static Gte of(String property, ScalarLiteral scalarLiteral) {
    return new ImmutableGte.Builder()
        .args(ImmutableList.of(Property.of(property), scalarLiteral))
        .build();
  }

  static Gte of(String property, String property2) {
    return new ImmutableGte.Builder()
        .args(ImmutableList.of(Property.of(property), Property.of(property2)))
        .build();
  }

  static Gte of(Property property, ScalarLiteral scalarLiteral) {
    return new ImmutableGte.Builder().args(ImmutableList.of(property, scalarLiteral)).build();
  }

  static Gte of(Property property, Property property2) {
    return new ImmutableGte.Builder().args(ImmutableList.of(property, property2)).build();
  }

  static Gte ofFunction(Function function, ScalarLiteral scalarLiteral) {
    return new ImmutableGte.Builder().args(ImmutableList.of(function, scalarLiteral)).build();
  }

  abstract class Builder extends BinaryScalarOperation.Builder<Gte> {}
}
