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
@JsonDeserialize(builder = ImmutableNot.Builder.class)
@FunctionalInterface
public interface Not extends UnaryOperation<Cql2Expression> {

  String TYPE = "not";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static Not of(Cql2Expression predicate) {
    return new ImmutableNot.Builder().addArgs(predicate).build();
  }

  static Not of(CqlPredicate predicate) {
    return new ImmutableNot.Builder().addArgs(Eq.of("", "")).build();
  }

  static Not of(NonBinaryScalarOperation scalarOperation) {
    return new ImmutableNot.Builder().addArgs(Eq.of("", "")).build();
  }
}
