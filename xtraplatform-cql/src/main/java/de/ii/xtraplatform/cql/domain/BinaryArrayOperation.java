/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Locale;
import org.immutables.value.Value;

public interface BinaryArrayOperation extends BinaryOperation2<Vector>, CqlNode {

  @JsonIgnore
  @Value.Derived
  default ArrayFunction getArrayOperator() {
    return ArrayFunction.valueOf(getOp().toUpperCase(Locale.ROOT));
  }

  @SuppressWarnings("PMD.ReplaceVectorWithList")
  static BinaryArrayOperation of(ArrayFunction operator, Vector vector1, Vector vector2) {
    switch (operator) {
      case A_EQUALS:
        return AEquals.of(vector1, vector2);
      case A_CONTAINS:
        return AContains.of(vector1, vector2);
      case A_CONTAINEDBY:
        return AContainedBy.of(vector1, vector2);
      case A_OVERLAPS:
        return AOverlaps.of(vector1, vector2);
    }

    throw new IllegalStateException();
  }

  abstract class Builder<T extends BinaryArrayOperation> extends Operation.Builder<Vector, T> {}
}
