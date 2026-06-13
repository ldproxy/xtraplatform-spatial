/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.jsonschema.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.Funnel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableJsonSchemaGeometry.Builder.class)
public abstract class JsonSchemaGeometry extends JsonSchema {

  @SuppressWarnings("UnstableApiUsage")
  public static final Funnel<JsonSchemaGeometry> FUNNEL =
      (from, into) -> {
        into.putString(from.getFormat(), StandardCharsets.UTF_8);
        from.getGeometryTypes()
            .ifPresent(types -> types.forEach(t -> into.putString(t, StandardCharsets.UTF_8)));
      };

  public abstract String getFormat();

  @JsonProperty("x-ldproxy-geometryTypes")
  public abstract Optional<List<String>> getGeometryTypes();

  public abstract static class Builder extends JsonSchema.Builder {}
}
