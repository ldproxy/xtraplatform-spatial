/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public interface LogicalOperation extends Operation<Cql2Expression> {

  @Value.Check
  default void check() {
    Preconditions.checkState(
        getArgs().size() > 1,
        "a boolean operation must have at least two children, found %s",
        getArgs().size());
  }

  @Override
  default <T> T accept(CqlVisitor<T> visitor) {
    List<T> children =
        getArgs().stream().map(predicate -> predicate.accept(visitor)).collect(Collectors.toList());

    return visitor.visit(this, children);
  }
}
