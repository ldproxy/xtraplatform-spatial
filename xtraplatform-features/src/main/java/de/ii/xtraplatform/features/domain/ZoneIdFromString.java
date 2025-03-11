/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.util.StdConverter;
import java.time.ZoneId;
import java.util.Optional;

public class ZoneIdFromString extends StdConverter<Optional<String>, Optional<ZoneId>> {

  public ZoneIdFromString() {
    super();
  }

  @Override
  public Optional<ZoneId> convert(Optional<String> value) {
    return value.map(ZoneId::of);
  }
}
