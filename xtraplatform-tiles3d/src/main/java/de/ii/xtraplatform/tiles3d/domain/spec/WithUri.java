/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain.spec;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.Funnel;
import java.nio.charset.StandardCharsets;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableWithUri.Builder.class)
public interface WithUri {

  @SuppressWarnings("UnstableApiUsage")
  Funnel<WithUri> FUNNEL =
      (from, into) -> {
        into.putString(from.getUri(), StandardCharsets.UTF_8);
      };

  String getUri();
}
