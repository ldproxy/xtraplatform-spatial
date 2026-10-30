/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

import de.ii.xtraplatform.cql.domain.And;
import de.ii.xtraplatform.cql.domain.BooleanValue2;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.CqlVisitorBase;
import de.ii.xtraplatform.cql.domain.IsNull;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.Or;
import de.ii.xtraplatform.cql.domain.Property;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rewrites a filter so that the given properties behave as if they were null in every row.
 *
 * <p>Predicates on such a property are resolved to a boolean literal following SQL semantics: `IS
 * NULL` holds, every other predicate is unknown. A WHERE clause treats unknown like false, but a
 * negation of unknown stays unknown, so negations are pushed down to the predicates before
 * resolving them.
 */
public class CqlAbsentPropertiesMapper {

  private final Set<String> absentProperties;

  public CqlAbsentPropertiesMapper(Set<String> absentProperties) {
    this.absentProperties = absentProperties;
  }

  public Cql2Expression map(Cql2Expression expression) {
    return map(expression, false);
  }

  private Cql2Expression map(Cql2Expression expression, boolean negated) {
    if (!referencesAbsentProperty(expression)) {
      return negated ? Not.of(expression) : expression;
    }
    if (expression instanceof Not) {
      return map(((Not) expression).getArgs().get(0), !negated);
    }
    if (expression instanceof And) {
      List<Cql2Expression> args = mapArgs(((And) expression).getArgs(), negated);
      return negated ? Or.of(args) : And.of(args);
    }
    if (expression instanceof Or) {
      List<Cql2Expression> args = mapArgs(((Or) expression).getArgs(), negated);
      return negated ? And.of(args) : Or.of(args);
    }

    return BooleanValue2.of(expression instanceof IsNull && !negated);
  }

  private List<Cql2Expression> mapArgs(List<Cql2Expression> args, boolean negated) {
    return args.stream().map(arg -> map(arg, negated)).collect(Collectors.toUnmodifiableList());
  }

  private boolean referencesAbsentProperty(Cql2Expression expression) {
    return expression.accept(new AbsentPropertyFinder(), true);
  }

  private class AbsentPropertyFinder extends CqlVisitorBase<Boolean> {

    private boolean found;

    @Override
    public Boolean postProcess(CqlNode node, Boolean ignore) {
      return found;
    }

    @Override
    public Boolean visit(Property property, List<Boolean> children) {
      String propertyName = property.getName().replaceAll("^\"|\"$", "");
      found |= absentProperties.contains(propertyName);
      return found;
    }
  }
}
