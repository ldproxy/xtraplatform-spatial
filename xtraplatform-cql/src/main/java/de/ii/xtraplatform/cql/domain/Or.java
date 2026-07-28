/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableOr.Builder.class)
@FunctionalInterface
public interface Or extends LogicalOperation {

  String TYPE = "or";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static Or of(Cql2Expression... predicates) {
    return new ImmutableOr.Builder().addArgs(predicates).build();
  }

  static Or of(List<Cql2Expression> predicates) {
    return new ImmutableOr.Builder().args(predicates).build();
  }
}
