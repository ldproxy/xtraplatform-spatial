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
@JsonDeserialize(builder = ImmutableAnd.Builder.class)
@FunctionalInterface
public interface And extends LogicalOperation {

  String TYPE = "and";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static And of(Cql2Expression... predicates) {
    return new ImmutableAnd.Builder().addArgs(predicates).build();
  }

  static And of(List<Cql2Expression> predicates) {
    return new ImmutableAnd.Builder().args(predicates).build();
  }
}
