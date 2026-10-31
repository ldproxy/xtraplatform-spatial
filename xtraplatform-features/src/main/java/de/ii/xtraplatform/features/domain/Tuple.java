/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface Tuple<T, U> {

  static <T, U> Tuple<T, U> of(T t, U u) {
    return ImmutableTuple.of(t, u);
  }

  @Nullable
  @Value.Parameter
  T first();

  @Nullable
  @Value.Parameter
  U second();
}
