/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import java.util.List;
import java.util.stream.Collectors;

@JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY, property = "op")
@JsonSubTypes({
  @Type(value = Eq.class, name = Eq.TYPE),
  @Type(value = Neq.class, name = Neq.TYPE),
  @Type(value = Gt.class, name = Gt.TYPE),
  @Type(value = Gte.class, name = Gte.TYPE),
  @Type(value = Lt.class, name = Lt.TYPE),
  @Type(value = Lte.class, name = Lte.TYPE),
  @Type(value = Like.class, name = Like.TYPE),
  @Type(value = Not.class, name = Not.TYPE),
  @Type(value = And.class, name = And.TYPE),
  @Type(value = Or.class, name = Or.TYPE),
  @Type(value = IsNull.class, name = IsNull.TYPE),
  @Type(value = In.class, name = In.TYPE),
  @Type(value = Between.class, name = Between.TYPE),
  @Type(value = TAfter.class, name = TAfter.TYPE),
  @Type(value = TBefore.class, name = TBefore.TYPE),
  @Type(value = TContains.class, name = TContains.TYPE),
  @Type(value = TDisjoint.class, name = TDisjoint.TYPE),
  @Type(value = TDuring.class, name = TDuring.TYPE),
  @Type(value = TEquals.class, name = TEquals.TYPE),
  @Type(value = TFinishedBy.class, name = TFinishedBy.TYPE),
  @Type(value = TFinishes.class, name = TFinishes.TYPE),
  @Type(value = TIntersects.class, name = TIntersects.TYPE),
  @Type(value = TMeets.class, name = TMeets.TYPE),
  @Type(value = TMetBy.class, name = TMetBy.TYPE),
  @Type(value = TOverlappedBy.class, name = TOverlappedBy.TYPE),
  @Type(value = TOverlaps.class, name = TOverlaps.TYPE),
  @Type(value = TStartedBy.class, name = TStartedBy.TYPE),
  @Type(value = TStarts.class, name = TStarts.TYPE),
  @Type(value = SContains.class, name = SContains.TYPE),
  @Type(value = SCrosses.class, name = SCrosses.TYPE),
  @Type(value = SDisjoint.class, name = SDisjoint.TYPE),
  @Type(value = SEquals.class, name = SEquals.TYPE),
  @Type(value = SIntersects.class, name = SIntersects.TYPE),
  @Type(value = SOverlaps.class, name = SOverlaps.TYPE),
  @Type(value = STouches.class, name = STouches.TYPE),
  @Type(value = SWithin.class, name = SWithin.TYPE),
  @Type(value = AContainedBy.class, name = AContainedBy.TYPE),
  @Type(value = AContains.class, name = AContains.TYPE),
  @Type(value = AEquals.class, name = AEquals.TYPE),
  @Type(value = AOverlaps.class, name = AOverlaps.TYPE)
})
public interface Operation<T extends Operand> extends Cql2Expression {

  String getOp();

  List<T> getArgs();

  abstract class Builder<T extends Operand, U extends Operation<T>> {

    public abstract U build();

    public abstract Builder<T, U> args(Iterable<? extends T> operands);

    public abstract Builder<T, U> addArgs(T operand);
  }

  @Override
  default <U> U accept(CqlVisitor<U> visitor) {
    List<U> operands =
        getArgs().stream().map(arg -> arg.accept(visitor)).collect(Collectors.toList());

    return visitor.visit(this, operands);
  }
}
