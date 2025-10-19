/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.jsonschema.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.Funnel;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableJsonSchemaAny.Builder.class)
public abstract class JsonSchemaAny extends JsonSchema {

  public static JsonSchemaAny of() {
    return new ImmutableJsonSchemaAny.Builder().build();
  }

  public abstract static class Builder extends JsonSchema.Builder {}

  @SuppressWarnings("UnstableApiUsage")
  public static final Funnel<JsonSchemaAny> FUNNEL = (from, into) -> {};
}
