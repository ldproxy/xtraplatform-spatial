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
@JsonDeserialize(builder = ImmutableAContainedBy.Builder.class)
@FunctionalInterface
public interface AContainedBy extends BinaryArrayOperation {

  String TYPE = "a_containedBy";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static AContainedBy of(Vector vector1, Vector vector2) {
    return new ImmutableAContainedBy.Builder().addArgs(vector1, vector2).build();
  }

  abstract class Builder extends BinaryArrayOperation.Builder<AContainedBy> {}
}
