/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import org.immutables.value.Value;

@FunctionalInterface
public interface BinaryOperation<T extends Literal> extends CqlNode {

  // @JsonValue
  List<Operand> getOperands();

  @Value.Check
  default void check() {
    int count = getOperands().size();
    Preconditions.checkState(
        count == 2, "a binary operation must have exactly two operands, found %s", count);
  }

  abstract class Builder<T extends Literal, U extends BinaryOperation<T>> {

    public abstract U build();

    // @JsonValue
    public abstract Builder<T, U> operands(Iterable<? extends Operand> operands);
  }

  @Override
  default <U> U accept(CqlVisitor<U> visitor) {
    U operand1 = getOperands().get(0).accept(visitor);
    U operand2 = getOperands().get(1).accept(visitor);

    return visitor.visit(this, Lists.newArrayList(operand1, operand2));
  }
}
