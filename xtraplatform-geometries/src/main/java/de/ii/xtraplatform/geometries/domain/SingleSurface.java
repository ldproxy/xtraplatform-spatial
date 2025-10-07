/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import java.util.List;
import org.immutables.value.Value;

public interface SingleSurface<T> extends Surface<List<T>> {

  @Value.Derived
  @Value.Auxiliary
  default int getNumRings() {
    if (isEmpty()) {
      return 0;
    }
    return getValue().size();
  }
}
