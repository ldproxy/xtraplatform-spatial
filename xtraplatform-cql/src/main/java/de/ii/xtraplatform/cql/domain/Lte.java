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
@JsonDeserialize(builder = ImmutableLte.Builder.class)
@FunctionalInterface
public interface Lte extends BinaryScalarOperation {

  String TYPE = "<=";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static Lte of(List<Scalar> operands) {
    return new ImmutableLte.Builder().args(operands).build();
  }

  static Lte of(String property, ScalarLiteral scalarLiteral) {
    return new ImmutableLte.Builder()
        .args(ImmutableList.of(Property.of(property), scalarLiteral))
        .build();
  }

  static Lte of(String property, String property2) {
    return new ImmutableLte.Builder()
        .args(ImmutableList.of(Property.of(property), Property.of(property2)))
        .build();
  }

  static Lte of(Property property, ScalarLiteral scalarLiteral) {
    return new ImmutableLte.Builder().args(ImmutableList.of(property, scalarLiteral)).build();
  }

  static Lte of(Property property, Property property2) {
    return new ImmutableLte.Builder().args(ImmutableList.of(property, property2)).build();
  }

  static Lte ofFunction(Function function, ScalarLiteral scalarLiteral) {
    return new ImmutableLte.Builder().args(ImmutableList.of(function, scalarLiteral)).build();
  }

  abstract class Builder extends BinaryScalarOperation.Builder<Lte> {}
}
