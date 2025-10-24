/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

import de.ii.xtraplatform.cql.domain.And;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.CqlVisitorCopy;
import de.ii.xtraplatform.cql.domain.Eq;
import de.ii.xtraplatform.cql.domain.Gt;
import de.ii.xtraplatform.cql.domain.Gte;
import de.ii.xtraplatform.cql.domain.Lt;
import de.ii.xtraplatform.cql.domain.Lte;
import de.ii.xtraplatform.cql.domain.Neq;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.Or;
import java.util.List;
import java.util.stream.Collectors;

public class CqlVisitorMapNots extends CqlVisitorCopy {

  @Override
  public CqlNode visit(Not not, List<CqlNode> children) {

    return map(not.getArgs().get(0));
  }

  private Cql2Expression map(Cql2Expression arg) {
    if (arg instanceof Or) {
      return And.of(
          ((Or) arg).getArgs().stream().map(this::map).collect(Collectors.toUnmodifiableList()));
    } else if (arg instanceof And) {
      return Or.of(
          ((And) arg).getArgs().stream().map(this::map).collect(Collectors.toUnmodifiableList()));
    } else if (arg instanceof Not) {
      return ((Not) arg).getArgs().get(0);
    } else if (arg instanceof Eq) {
      return Neq.of(((Eq) arg).getArgs());
    } else if (arg instanceof Neq) {
      return Eq.of(((Neq) arg).getArgs());
    } else if (arg instanceof Lt) {
      return Gte.of(((Lt) arg).getArgs());
    } else if (arg instanceof Gt) {
      return Lte.of(((Gt) arg).getArgs());
    } else if (arg instanceof Lte) {
      return Gt.of(((Lte) arg).getArgs());
    } else if (arg instanceof Gte) {
      return Lt.of(((Gte) arg).getArgs());
    }
    return Not.of(arg);
  }
}
