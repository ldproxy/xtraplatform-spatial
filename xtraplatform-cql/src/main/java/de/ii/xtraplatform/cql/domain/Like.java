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
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableLike.Builder.class)
@FunctionalInterface
public interface Like extends BinaryScalarOperation {

  String TYPE = "like";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static Like of(String property, ScalarLiteral scalarLiteral) {
    return new ImmutableLike.Builder()
        .args(ImmutableList.of(Property.of(property), scalarLiteral))
        .build();
  }

  static Like of(String property, String property2) {
    return new ImmutableLike.Builder()
        .args(ImmutableList.of(Property.of(property), Property.of(property2)))
        .build();
  }

  static Like ofFunction(Function function, ScalarLiteral scalarLiteral) {
    return new ImmutableLike.Builder().args(ImmutableList.of(function, scalarLiteral)).build();
  }

  abstract class Builder extends BinaryScalarOperation.Builder<Like> {}
}
