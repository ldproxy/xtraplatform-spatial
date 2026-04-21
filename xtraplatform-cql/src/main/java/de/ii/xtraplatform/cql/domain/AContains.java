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
@JsonDeserialize(builder = ImmutableAContains.Builder.class)
public interface AContains extends BinaryArrayOperation, CqlNode {

  String TYPE = "a_contains";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  @SuppressWarnings("PMD.ReplaceVectorWithList")
  static AContains of(Vector vector1, Vector vector2) {
    return new ImmutableAContains.Builder().addArgs(vector1, vector2).build();
  }

  abstract class Builder extends BinaryArrayOperation.Builder<AContains> {}
}
