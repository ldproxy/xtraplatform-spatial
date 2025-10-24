/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.cql.domain.And;
import de.ii.xtraplatform.cql.domain.BinaryTemporalOperation;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.CqlVisitorCopy;
import de.ii.xtraplatform.cql.domain.Eq;
import de.ii.xtraplatform.cql.domain.Function;
import de.ii.xtraplatform.cql.domain.Gt;
import de.ii.xtraplatform.cql.domain.Gte;
import de.ii.xtraplatform.cql.domain.ImmutableEq;
import de.ii.xtraplatform.cql.domain.Lt;
import de.ii.xtraplatform.cql.domain.Lte;
import de.ii.xtraplatform.cql.domain.Neq;
import de.ii.xtraplatform.cql.domain.Operand;
import de.ii.xtraplatform.cql.domain.Or;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.Temporal;
import de.ii.xtraplatform.cql.domain.TemporalFunction;
import de.ii.xtraplatform.cql.domain.TemporalLiteral;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.threeten.extra.Interval;

public class CqlVisitorMapTemporalOperators extends CqlVisitorCopy {

  private final Set<TemporalFunction> supportedOperators;

  public CqlVisitorMapTemporalOperators(Set<TemporalFunction> supportedOperators) {
    this.supportedOperators = supportedOperators;
  }

  @Override
  public CqlNode visit(BinaryTemporalOperation temporalOperation, List<CqlNode> children) {

    Temporal temporal1 = (Temporal) children.get(0);
    Temporal temporal2 = (Temporal) children.get(1);

    TemporalFunction temporalFunction =
        TemporalFunction.valueOf(temporalOperation.getOp().toUpperCase());

    // if the next visitor supports a temporal operator, we keep it
    if (supportedOperators.contains(temporalFunction)) {
      return BinaryTemporalOperation.of(temporalFunction, temporal1, temporal2);
    }

    Optional<Class<?>> granularity = getGranularity(temporal1, temporal2);

    // otherwise we simplify the predicate to scalar comparisons
    switch (temporalFunction) {
      case T_AFTER:
        // this operator accepts not only intervals, but also instants
        if (instantsOfSameGranularity(temporal1, temporal2)) {
          // for instants of the same type, this is just >
          return Gt.of(ImmutableList.of(temporal1, temporal2));
        }
        // start1 > end2
        return Gt.of(
            ImmutableList.of(getStart(temporal1, granularity), getEnd(temporal2, granularity)));
      case T_BEFORE:
        // this operator accepts not only intervals, but also instants
        if (instantsOfSameGranularity(temporal1, temporal2)) {
          // for instants of the same type, this is just <
          return Lt.of(ImmutableList.of(temporal1, temporal2));
        }
        // end1 < start2
        return Lt.of(
            ImmutableList.of(getEnd(temporal1, granularity), getStart(temporal2, granularity)));
      case T_DURING:
        // start1 > start2 AND end1 < end2
        return And.of(
            Gt.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getStart(temporal2, granularity))),
            Lt.of(
                ImmutableList.of(getEnd(temporal1, granularity), getEnd(temporal2, granularity))));
      case T_EQUALS:
        // this operator accepts not only intervals, but also instants
        if (instantsOfSameGranularity(temporal1, temporal2)) {
          // for instants of the same type, this is just =
          return Eq.of(ImmutableList.of(temporal1, temporal2));
        }
        // start1 = start2 AND end1 = end2
        return And.of(
            Eq.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getStart(temporal2, granularity))),
            Eq.of(
                ImmutableList.of(getEnd(temporal1, granularity), getEnd(temporal2, granularity))));
      case T_STARTS:
        // start1 = start2 AND end1 < end2
        return And.of(
            Eq.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getStart(temporal2, granularity))),
            Lt.of(
                ImmutableList.of(getEnd(temporal1, granularity), getEnd(temporal2, granularity))));
      case T_STARTEDBY:
        // start1 = start2 AND end1 > end2
        return And.of(
            Eq.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getStart(temporal2, granularity))),
            Gt.of(
                ImmutableList.of(getEnd(temporal1, granularity), getEnd(temporal2, granularity))));
      case T_CONTAINS:
        // start1 < start2 AND end1 > end2
        return And.of(
            Lt.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getStart(temporal2, granularity))),
            Gt.of(
                ImmutableList.of(getEnd(temporal1, granularity), getEnd(temporal2, granularity))));
      case T_DISJOINT:
        // this operator accepts not only intervals, but also instants
        if (instantsOfSameGranularity(temporal1, temporal2)) {
          // for instants of the same type, this is just <>
          return Neq.of(ImmutableList.of(temporal1, temporal2));
        }
        // end1 < start2 OR start1 > end2
        return Or.of(
            Lt.of(
                ImmutableList.of(getEnd(temporal1, granularity), getStart(temporal2, granularity))),
            Gt.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getEnd(temporal2, granularity))));
      case T_INTERSECTS:
        // this operator accepts not only intervals, but also instants
        if (instantsOfSameGranularity(temporal1, temporal2)) {
          // for instants of the same type, this is just =
          return Eq.of(ImmutableList.of(temporal1, temporal2));
        }
        // NOT (end1 < start2 OR start1 > end2)
        // to avoid issues with properties that are null evaluate as (using De Morgan's laws)
        // end1 >= start2 AND start1 <= end2
        return And.of(
            Gte.of(
                ImmutableList.of(getEnd(temporal1, granularity), getStart(temporal2, granularity))),
            Lte.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getEnd(temporal2, granularity))));
      case T_FINISHES:
        // start1 > start2 AND end1 = end2
        return And.of(
            Gt.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getStart(temporal2, granularity))),
            Eq.of(
                ImmutableList.of(getEnd(temporal1, granularity), getEnd(temporal2, granularity))));
      case T_FINISHEDBY:
        // start1 < start2 AND end1 = end2
        return And.of(
            Lt.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getStart(temporal2, granularity))),
            Eq.of(
                ImmutableList.of(getEnd(temporal1, granularity), getEnd(temporal2, granularity))));
      case T_MEETS:
        // end1 = start2
        return new ImmutableEq.Builder()
            .addArgs(getEnd(temporal1, granularity), getStart(temporal2, granularity))
            .build();
      case T_METBY:
        // start1 = end2
        return new ImmutableEq.Builder()
            .addArgs(getStart(temporal1, granularity), getEnd(temporal2, granularity))
            .build();
      case T_OVERLAPS:
        // start1 < start2 AND end1 > start2 AND end1 < end2
        return And.of(
            Lt.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getStart(temporal2, granularity))),
            Gt.of(
                ImmutableList.of(getEnd(temporal1, granularity), getStart(temporal2, granularity))),
            Lt.of(
                ImmutableList.of(getEnd(temporal1, granularity), getEnd(temporal2, granularity))));
      case T_OVERLAPPEDBY:
        // start1 > start2 AND start1 < end2 AND end1 > end2
        return And.of(
            Gt.of(
                ImmutableList.of(
                    getStart(temporal1, granularity), getStart(temporal2, granularity))),
            Lt.of(
                ImmutableList.of(getStart(temporal1, granularity), getEnd(temporal2, granularity))),
            Gt.of(
                ImmutableList.of(getEnd(temporal1, granularity), getEnd(temporal2, granularity))));
    }

    throw new IllegalStateException("unknown temporal operator: " + temporalFunction);
  }

  private boolean instantsOfSameGranularity(Temporal temporal1, Temporal temporal2) {

    // try to determine the data types of the operands
    // for properties, it is either date or timestamp, but we do not have the type information at
    // this point

    Class<?> type1 = null;
    if (temporal1 instanceof TemporalLiteral) {
      type1 = ((TemporalLiteral) temporal1).getType();
    }

    Class<?> type2 = null;
    if (temporal2 instanceof TemporalLiteral) {
      type2 = ((TemporalLiteral) temporal2).getType();
    }

    // if one is a property and the other a literal instant, we assume that both are of the same
    // type
    if (temporal1 instanceof Property && Objects.nonNull(type2)) {
      if (type2.equals(LocalDate.class) || type2.equals(Instant.class)) {
        type1 = type2;
      }
    } else if (Objects.nonNull(type1) && temporal2 instanceof Property) {
      if (type1.equals(LocalDate.class) || type1.equals(Instant.class)) {
        type2 = type1;
      }
    }

    // fold intervals
    if (de.ii.xtraplatform.cql.domain.Interval.class.equals(type1)) type1 = Interval.class;
    if (de.ii.xtraplatform.cql.domain.Interval.class.equals(type2)) type2 = Interval.class;

    return Objects.nonNull(type1)
        && Objects.nonNull(type2)
        && type1.equals(type2)
        && !type1.equals(Interval.class);
  }

  private Optional<Class<?>> getGranularity(Temporal temporal1, Temporal temporal2) {

    // try to determine the data types of the operands
    // for properties, it is either date or timestamp, but we do not have the type information at
    // this point

    Class<?> type1 = null;
    if (temporal1 instanceof TemporalLiteral) {
      if (((TemporalLiteral) temporal1)
          .getType()
          .equals(de.ii.xtraplatform.cql.domain.Interval.class)) {
        List<Operand> arguments =
            ((de.ii.xtraplatform.cql.domain.Interval) ((TemporalLiteral) temporal1).getValue())
                .getArgs();
        if (arguments.stream()
            .anyMatch(
                arg ->
                    arg instanceof TemporalLiteral
                        && ((TemporalLiteral) arg).getType().equals(LocalDate.class))) {
          type1 = LocalDate.class;
        } else if (arguments.stream()
            .anyMatch(
                arg ->
                    arg instanceof TemporalLiteral
                        && ((TemporalLiteral) arg).getType().equals(Instant.class))) {
          type1 = Instant.class;
        }
      } else {
        type1 = ((TemporalLiteral) temporal1).getType();
      }
    }

    Class<?> type2 = null;
    if (temporal2 instanceof TemporalLiteral) {
      if (((TemporalLiteral) temporal2)
          .getType()
          .equals(de.ii.xtraplatform.cql.domain.Interval.class)) {
        List<Operand> arguments =
            ((de.ii.xtraplatform.cql.domain.Interval) ((TemporalLiteral) temporal2).getValue())
                .getArgs();
        if (arguments.stream()
            .anyMatch(
                arg ->
                    arg instanceof TemporalLiteral
                        && ((TemporalLiteral) arg).getType().equals(LocalDate.class))) {
          type2 = LocalDate.class;
        } else if (arguments.stream()
            .anyMatch(
                arg ->
                    arg instanceof TemporalLiteral
                        && ((TemporalLiteral) arg).getType().equals(Instant.class))) {
          type2 = Instant.class;
        }
      } else {
        type2 = ((TemporalLiteral) temporal2).getType();
      }
    }

    if (Objects.isNull(type1) && Objects.isNull(type2)) {
      // we have no indication
      return Optional.empty();
    } else if (Objects.isNull(type1)) {
      // if one is unknown, we assume that both are of the same granularity
      return Optional.of(type2);
    } else if (Objects.isNull(type2)) {
      // if one is unknown, we assume that both are of the same granularity
      return Optional.of(type1);
    } else if (type1.equals(type2)) {
      return Optional.of(type1);
    }

    return Optional.empty();
  }

  private Temporal getStart(
      Temporal temporal, @SuppressWarnings("unused") Optional<Class<?>> granularity) {
    if (temporal instanceof Property) {
      return temporal;
    } else if (temporal instanceof TemporalLiteral) {
      if (((TemporalLiteral) temporal).getType() == Interval.class) {
        return TemporalLiteral.of(((Interval) ((TemporalLiteral) temporal).getValue()).getStart());
      } else if (((TemporalLiteral) temporal).getType() == TemporalLiteral.OPEN.class) {
        return TemporalLiteral.of(Instant.MIN);
      } else if (((TemporalLiteral) temporal).getType()
          == de.ii.xtraplatform.cql.domain.Interval.class) {
        de.ii.xtraplatform.cql.domain.Interval interval =
            (de.ii.xtraplatform.cql.domain.Interval) ((TemporalLiteral) temporal).getValue();
        return getStart((Temporal) interval.getArgs().get(0), granularity);
      }
      return temporal;
    } else if (temporal instanceof de.ii.xtraplatform.cql.domain.Interval) {
      de.ii.xtraplatform.cql.domain.Interval interval =
          ((de.ii.xtraplatform.cql.domain.Interval) temporal);
      return getStart((Temporal) interval.getArgs().get(0), granularity);
    } else if (temporal instanceof Function) {
      Temporal start = (Temporal) ((Function) temporal).getArgs().get(0);
      if (start instanceof TemporalLiteral
          && ((TemporalLiteral) start).getType() == TemporalLiteral.OPEN.class) {
        return TemporalLiteral.of(Instant.MIN);
      }
      return start;
    }

    throw new IllegalStateException(
        "unknown temporal type: " + temporal.getClass().getSimpleName());
  }

  private Temporal getEnd(Temporal temporal, Optional<Class<?>> granularity) {
    if (temporal instanceof Property) {
      return temporal;
    } else if (temporal instanceof TemporalLiteral) {
      if (((TemporalLiteral) temporal).getType() == Interval.class) {
        Instant end = ((Interval) ((TemporalLiteral) temporal).getValue()).getEnd();
        return TemporalLiteral.of(end);
      } else if (((TemporalLiteral) temporal).getType() == TemporalLiteral.OPEN.class) {
        return TemporalLiteral.of(Instant.MAX);
      } else if (((TemporalLiteral) temporal).getType()
          == de.ii.xtraplatform.cql.domain.Interval.class) {
        de.ii.xtraplatform.cql.domain.Interval interval =
            (de.ii.xtraplatform.cql.domain.Interval) ((TemporalLiteral) temporal).getValue();
        return getEnd((Temporal) interval.getArgs().get(1), granularity);
      } else if (((TemporalLiteral) temporal).getType() == LocalDate.class) {
        // if we know that the temporal granularity is "day", we return the value
        if (granularity.isPresent() && LocalDate.class.equals(granularity.get())) {
          return temporal;
        }
        // otherwise we assume "second" and convert to last second of the day,
        // since we currently may not know, whether we are comparing against a timestamp or a date;
        // if we would know the data type of the other operand this could be improved
        return TemporalLiteral.of(
            ((LocalDate) ((TemporalLiteral) temporal).getValue())
                .atTime(23, 59, 59)
                .atZone(ZoneOffset.UTC)
                .toInstant());
      }
      return temporal;
    } else if (temporal instanceof de.ii.xtraplatform.cql.domain.Interval) {
      de.ii.xtraplatform.cql.domain.Interval interval =
          ((de.ii.xtraplatform.cql.domain.Interval) temporal);
      return getEnd((Temporal) interval.getArgs().get(1), granularity);
    } else if (temporal instanceof Function) {
      Temporal end = (Temporal) ((Function) temporal).getArgs().get(1);
      if (end instanceof TemporalLiteral
          && ((TemporalLiteral) end).getType() == TemporalLiteral.OPEN.class) {
        return TemporalLiteral.of(Instant.MAX);
      }
      return end;
    }

    throw new IllegalStateException(
        "unknown temporal type: " + temporal.getClass().getSimpleName());
  }
}
