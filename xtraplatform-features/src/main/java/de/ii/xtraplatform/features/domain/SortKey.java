/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import org.immutables.value.Value;
import org.immutables.value.Value.Default;

@Value.Immutable
@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface SortKey {

  enum Direction {
    ASCENDING,
    DESCENDING
  }

  static SortKey of(String field) {
    return ImmutableSortKey.builder().field(field).build();
  }

  static SortKey of(String field, Direction direction) {
    return ImmutableSortKey.builder().field(field).direction(direction).build();
  }

  String getField();

  @Default
  default Direction getDirection() {
    return Direction.ASCENDING;
  }
}
