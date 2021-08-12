/**
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.domain.schema;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

import java.util.List;
import java.util.Map;

@Value.Immutable
@Value.Style(jdkOnly = true, deepImmutablesDetection = true)
@JsonDeserialize(as = ImmutableJsonSchemaObject.class)
public abstract class JsonSchemaObject extends JsonSchema {

    public final String getType() { return "object"; }

    public abstract List<String> getRequired();
    public abstract Map<String, JsonSchema> getProperties();
    public abstract Map<String, JsonSchema> getPatternProperties();

}
