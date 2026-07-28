/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@FunctionalInterface
@JsonDeserialize(builder = ImmutableSEquals.Builder.class)
public interface SEquals extends BinarySpatialOperation {

  String TYPE = "s_equals";

  @Override
  @Value.Derived
  default String getOp() {
    return TYPE;
  }

  static SEquals of(Spatial spatial1, Spatial spatial2) {
    return new ImmutableSEquals.Builder().addArgs(spatial1, spatial2).build();
  }

  abstract class Builder extends BinarySpatialOperation.Builder<SEquals> {}
}
