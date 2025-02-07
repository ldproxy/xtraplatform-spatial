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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableIn.Builder.class)
public interface In extends BinaryOperation2<Scalar>, CqlNode {

  String TYPE = "in";
  String ID_PLACEHOLDER = "_ID_";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isIdFilter() {
    return !getArgs().isEmpty()
        && getArgs().get(0) instanceof Property
        && Objects.equals(((Property) getArgs().get(0)).getName(), ID_PLACEHOLDER);
  }

  static In of(String property, List<Scalar> values) {
    return new ImmutableIn.Builder()
        .addArgs(Property.of(property))
        .addArgs(ArrayLiteral.of(values))
        .build();
  }

  static In of(String property, Scalar... values) {
    return new ImmutableIn.Builder()
        .addArgs(Property.of(property))
        .addArgs(ArrayLiteral.of(Arrays.asList(values)))
        .build();
  }

  static In of(String property, TemporalLiteral... values) {
    return new ImmutableIn.Builder()
        .addArgs(Property.of(property))
        .addArgs(ArrayLiteral.of(Arrays.asList(values)))
        .build();
  }

  static In of(Scalar... values) {
    return new ImmutableIn.Builder()
        .addArgs(Property.of(ID_PLACEHOLDER))
        .addArgs(ArrayLiteral.of(Arrays.asList(values)))
        .build();
  }

  static In of(List<Scalar> values) {
    return new ImmutableIn.Builder()
        .addArgs(Property.of(ID_PLACEHOLDER))
        .addArgs(ArrayLiteral.of(values))
        .build();
  }

  static In of(Scalar leftOperand, List<Scalar> values) {
    return new ImmutableIn.Builder().addArgs(leftOperand).addArgs(ArrayLiteral.of(values)).build();
  }

  static In ofFunction(Function function, List<Scalar> values) {
    return new ImmutableIn.Builder().addArgs(function).addArgs(ArrayLiteral.of(values)).build();
  }

  abstract class Builder extends BinaryOperation2.Builder<Scalar, In> {}
}
