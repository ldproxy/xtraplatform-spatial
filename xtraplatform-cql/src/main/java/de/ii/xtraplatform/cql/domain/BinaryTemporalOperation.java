/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Locale;
import org.immutables.value.Value;
import org.threeten.extra.Interval;

public interface BinaryTemporalOperation extends BinaryOperation2<Temporal> {

  List<TemporalFunction> INTERVAL_ONLY =
      ImmutableList.of(
          TemporalFunction.T_CONTAINS,
          TemporalFunction.T_DURING,
          TemporalFunction.T_FINISHEDBY,
          TemporalFunction.T_FINISHES,
          TemporalFunction.T_MEETS,
          TemporalFunction.T_METBY,
          TemporalFunction.T_OVERLAPPEDBY,
          TemporalFunction.T_OVERLAPS,
          TemporalFunction.T_STARTEDBY,
          TemporalFunction.T_STARTS);

  @JsonIgnore
  @Value.Derived
  default TemporalFunction getTemporalOperator() {
    return TemporalFunction.valueOf(getOp().toUpperCase(Locale.ROOT));
  }

  @SuppressWarnings("PMD.CyclomaticComplexity")
  static BinaryTemporalOperation of(
      TemporalFunction operator, Temporal temporal1, Temporal temporal2) {
    switch (operator) {
      case T_AFTER:
        return TAfter.of(temporal1, temporal2);
      case T_BEFORE:
        return TBefore.of(temporal1, temporal2);
      case T_CONTAINS:
        return TContains.of(temporal1, temporal2);
      case T_DISJOINT:
        return TDisjoint.of(temporal1, temporal2);
      case T_DURING:
        return TDuring.of(temporal1, temporal2);
      case T_EQUALS:
        return TEquals.of(temporal1, temporal2);
      case T_FINISHEDBY:
        return TFinishedBy.of(temporal1, temporal2);
      case T_FINISHES:
        return TFinishes.of(temporal1, temporal2);
      case T_INTERSECTS:
        return TIntersects.of(temporal1, temporal2);
      case T_MEETS:
        return TMeets.of(temporal1, temporal2);
      case T_METBY:
        return TMetBy.of(temporal1, temporal2);
      case T_OVERLAPPEDBY:
        return TOverlappedBy.of(temporal1, temporal2);
      case T_OVERLAPS:
        return TOverlaps.of(temporal1, temporal2);
      case T_STARTEDBY:
        return TStartedBy.of(temporal1, temporal2);
      case T_STARTS:
        return TStarts.of(temporal1, temporal2);
    }

    throw new IllegalStateException();
  }

  @Value.Check
  @Override
  default void check() {
    BinaryOperation2.super.check();
    getArgs()
        .forEach(
            operand -> {
              Preconditions.checkState(
                  operand != null, "a temporal operation must have temporal operands, found null");
              if (INTERVAL_ONLY.contains(getTemporalOperator())) {
                if (operand instanceof Property) {
                  Preconditions.checkState(
                      true,
                      "The arguments of %s must be an INTERVAL(). Found: a property ('%s').",
                      getTemporalOperator().toString(),
                      ((Property) operand).getName());
                }
                if (operand instanceof TemporalLiteral) {
                  Preconditions.checkState(
                      ((TemporalLiteral) operand).getType() == Interval.class
                          || ((TemporalLiteral) operand).getType()
                              == de.ii.xtraplatform.cql.domain.Interval.class,
                      "The arguments of %s must be an INTERVAL(). Found: a temporal literal '%s' of type '%s'.",
                      getTemporalOperator().toString(),
                      ((TemporalLiteral) operand).getValue(),
                      ((TemporalLiteral) operand).getType());
                }
              }
            });
  }

  abstract class Builder<T extends BinaryTemporalOperation>
      extends Operation.Builder<Temporal, T> {}
}
