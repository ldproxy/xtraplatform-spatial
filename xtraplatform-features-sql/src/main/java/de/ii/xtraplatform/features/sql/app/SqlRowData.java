/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import java.util.Map;
import org.immutables.value.Value;

@Value.Modifiable
public interface SqlRowData {

  Map<String, String> getIds();

  Map<String, String> getValues();

  SqlRowData putIds(String key, String value);

  @Value.Lazy
  default boolean isEmpty() {
    return getIds().isEmpty() && getValues().isEmpty();
  }
}
