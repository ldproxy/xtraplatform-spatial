/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Lists;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableCqlFilter.Builder.class)
public interface CqlFilter extends CqlPredicate {

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  static CqlFilter of(CqlNode node) {
    if (node instanceof CqlFilter) {
      return of(((CqlFilter) node).getExpressions().get(0));
    } else if (node instanceof CqlPredicate) {
      return of(((CqlPredicate) node).getExpressions().get(0));
    }

    ImmutableCqlFilter.Builder builder = new ImmutableCqlFilter.Builder();

    if (node instanceof And) {
      builder.and((And) node);
    } else if (node instanceof Or) {
      builder.or((Or) node);
    } else if (node instanceof Not) {
      builder.not((Not) node);
    } else if (node instanceof ScalarLiteral
        && ((ScalarLiteral) node).getType().equals(BooleanValue2.class)) {
      builder.booleanValue((ScalarLiteral) node);
    } else if (node instanceof Eq) {
      builder.eq((Eq) node);
    } else if (node instanceof Neq) {
      builder.neq((Neq) node);
    } else if (node instanceof Gt) {
      builder.gt((Gt) node);
    } else if (node instanceof Gte) {
      builder.gte((Gte) node);
    } else if (node instanceof Lt) {
      builder.lt((Lt) node);
    } else if (node instanceof Lte) {
      builder.lte((Lte) node);
    } else if (node instanceof Between) {
      builder.between((Between) node);
    } else if (node instanceof In) {
      builder.inOperator((In) node);
    } else if (node instanceof Like) {
      builder.like((Like) node);
    } else if (node instanceof IsNull) {
      builder.isNull((IsNull) node);
    } else if (node instanceof TemporalOperation) {
      builder.temporalOperation((TemporalOperation) node);
    } else if (node instanceof SpatialOperation) {
      builder.spatialOperation((SpatialOperation) node);
    } else if (node instanceof ArrayOperation) {
      builder.arrayOperation((ArrayOperation) node);
    }

    return builder.build();
  }

  @Override
  default <T> T accept(CqlVisitor<T> visitor) {
    T expression = getExpressions().get(0).accept(visitor);
    return visitor.visit(this, Lists.newArrayList(expression));
  }
}
