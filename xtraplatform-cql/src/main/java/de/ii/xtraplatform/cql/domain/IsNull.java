/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableIsNull.Builder.class)
@FunctionalInterface
public interface IsNull extends UnaryOperation<Scalar> {

  String TYPE = "isNull";
  String TEXT = "IS NULL";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static IsNull of(String property) {
    return new ImmutableIsNull.Builder().addArgs(Property.of(property)).build();
  }

  static IsNull of(Property property) {
    return new ImmutableIsNull.Builder().addArgs(property).build();
  }

  static IsNull of(Function function) {
    return new ImmutableIsNull.Builder().addArgs(function).build();
  }

  abstract class Builder extends Operation.Builder<Scalar, IsNull> {}
}
