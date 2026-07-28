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
@JsonDeserialize(builder = ImmutableEq.Builder.class)
@FunctionalInterface
public interface Eq extends BinaryScalarOperation {

  String TYPE = "=";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static Eq of(List<Scalar> operands) {
    return new ImmutableEq.Builder().args(operands).build();
  }

  static Eq of(String property, ScalarLiteral scalarLiteral) {
    return new ImmutableEq.Builder()
        .args(ImmutableList.of(Property.of(property), scalarLiteral))
        .build();
  }

  static Eq of(String property, TemporalLiteral temporalLiteral) {
    return new ImmutableEq.Builder()
        .args(ImmutableList.of(Property.of(property), temporalLiteral))
        .build();
  }

  static Eq of(String property, String property2) {
    return new ImmutableEq.Builder()
        .args(ImmutableList.of(Property.of(property), Property.of(property2)))
        .build();
  }

  static Eq of(Property property, ScalarLiteral scalarLiteral) {
    return new ImmutableEq.Builder().args(ImmutableList.of(property, scalarLiteral)).build();
  }

  static Eq of(Property property, Property property2) {
    return new ImmutableEq.Builder().args(ImmutableList.of(property, property2)).build();
  }

  static Eq ofFunction(Function function, ScalarLiteral scalarLiteral) {
    return new ImmutableEq.Builder().args(ImmutableList.of(function, scalarLiteral)).build();
  }

  abstract class Builder extends BinaryScalarOperation.Builder<Eq> {}
}
