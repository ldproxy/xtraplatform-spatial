/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableBetween.Builder.class)
@FunctionalInterface
public interface Between extends TernaryOperation<Scalar> {

  String TYPE = "between";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static Between of(String property, ScalarLiteral scalarLiteral1, ScalarLiteral scalarLiteral2) {
    return new ImmutableBetween.Builder()
        .addArgs(Property.of(property))
        .addArgs(scalarLiteral1)
        .addArgs(scalarLiteral2)
        .build();
  }

  static Between of(Property property, ScalarLiteral scalarLiteral1, ScalarLiteral scalarLiteral2) {
    return new ImmutableBetween.Builder()
        .addArgs(property)
        .addArgs(scalarLiteral1)
        .addArgs(scalarLiteral2)
        .build();
  }

  static Between ofFunction(
      Function function, ScalarLiteral scalarLiteral1, ScalarLiteral scalarLiteral2) {
    return new ImmutableBetween.Builder()
        .addArgs(function)
        .addArgs(scalarLiteral1)
        .addArgs(scalarLiteral2)
        .build();
  }

  @JsonIgnore
  @Value.Derived
  default Optional<Scalar> getValue() {
    return Optional.ofNullable(getArgs().get(0));
  }

  @JsonIgnore
  @Value.Derived
  default Optional<Scalar> getLower() {
    return Optional.ofNullable(getArgs().get(1));
  }

  @JsonIgnore
  @Value.Derived
  default Optional<Scalar> getUpper() {
    return Optional.ofNullable(getArgs().get(2));
  }

  abstract class Builder extends TernaryOperation.Builder<Scalar, Between> {}
}
