/**
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.domain.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Optional;
import org.immutables.value.Value;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NONE)
@JsonSubTypes({
        @JsonSubTypes.Type(value = JsonSchemaString.class, name = "string"),
        @JsonSubTypes.Type(value = JsonSchemaNumber.class, name = "number"),
        @JsonSubTypes.Type(value = JsonSchemaInteger.class, name = "integer"),
        @JsonSubTypes.Type(value = JsonSchemaBoolean.class, name = "boolean"),
        @JsonSubTypes.Type(value = JsonSchemaObject.class, name = "object"),
        @JsonSubTypes.Type(value = JsonSchemaArray.class, name = "array"),
        @JsonSubTypes.Type(value = JsonSchemaNull.class, name = "null"),
        @JsonSubTypes.Type(value = JsonSchemaRef.class, name = "$ref")
})
public abstract class JsonSchema {

    public abstract Optional<String> getTitle();
    public abstract Optional<String> getDescription();

    @JsonIgnore
    @Value.Auxiliary
    public abstract Optional<String> getName();

    @JsonIgnore
    @Value.Default
    @Value.Auxiliary
    public boolean isRequired() {
        return false;
    }
}
