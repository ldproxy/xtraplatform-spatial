/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import de.ii.xtraplatform.geometries.domain.Position;
import org.immutables.value.Value;

@Value.Immutable
public interface PositionNode extends CqlNode {

  @Value.Parameter
  Position getPosition();

  static PositionNode of(Position position) {
    return ImmutablePositionNode.of(position);
  }
}
