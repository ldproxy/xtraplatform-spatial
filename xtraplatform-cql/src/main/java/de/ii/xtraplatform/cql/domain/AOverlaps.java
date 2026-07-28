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
@JsonDeserialize(builder = ImmutableAOverlaps.Builder.class)
@FunctionalInterface
public interface AOverlaps extends BinaryArrayOperation {

  String TYPE = "a_overlaps";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static AOverlaps of(Vector vector1, Vector vector2) {
    return new ImmutableAOverlaps.Builder().addArgs(vector1, vector2).build();
  }

  abstract class Builder extends BinaryArrayOperation.Builder<AOverlaps> {}
}
