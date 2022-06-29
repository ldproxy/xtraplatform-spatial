/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

public interface Cql2Expression extends Operand, CqlNode {

  default <T> T accept(CqlVisitor<T> visitor, boolean isRoot) {
    T visited = accept(visitor);
    return isRoot ? visitor.postProcess(this, visited) : visited;
  }
}
