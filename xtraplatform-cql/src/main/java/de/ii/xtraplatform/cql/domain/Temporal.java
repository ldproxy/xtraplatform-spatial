/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.google.common.collect.ImmutableList;

public interface Temporal extends Scalar {

  @SuppressWarnings("PMD.CyclomaticComplexity")
  static Temporal intervalOf(Temporal op1, Temporal op2) {
    // if at least one parameter is a property, we create a function, otherwise a fixed interval
    if (op1 instanceof Property && op2 instanceof Property) {
      return Interval.of(ImmutableList.of((Property) op1, (Property) op2));
    } else if (op1 instanceof Property && op2 instanceof TemporalLiteral) {
      return Interval.of(ImmutableList.of((Property) op1, (TemporalLiteral) op2));
    } else if (op1 instanceof TemporalLiteral && op2 instanceof Property) {
      return Interval.of(ImmutableList.of((TemporalLiteral) op1, (Property) op2));
    } else if (op1 instanceof TemporalLiteral && op2 instanceof TemporalLiteral) {
      return TemporalLiteral.of((TemporalLiteral) op1, (TemporalLiteral) op2);
    }

    throw new IllegalStateException(
        String.format("unsupported interval operands: %s, %s", op1.getClass(), op2.getClass()));
  }
}
