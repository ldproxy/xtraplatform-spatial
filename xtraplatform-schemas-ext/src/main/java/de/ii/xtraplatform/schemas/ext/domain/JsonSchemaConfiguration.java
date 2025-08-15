/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.schemas.ext.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.JsonDynamicSubType;
import de.ii.xtraplatform.features.domain.ExtensionConfiguration;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import java.util.Map;
import org.immutables.value.Value;

/**
 * @type JSON_SCHEMA
 * @langAll <code>
 * ```yaml
 * - type: JSON_SCHEMA
 *   enabled: true
 *   objectTypeRefs:
 *     '#/$defs/Link': Link
 * ```
 * </code>
 */
@Value.Immutable
@Value.Style(builder = "new")
@JsonDynamicSubType(superType = ExtensionConfiguration.class, id = "JSON_SCHEMA")
@JsonDeserialize(builder = ImmutableJsonSchemaConfiguration.Builder.class)
public interface JsonSchemaConfiguration extends ExtensionConfiguration {

  /**
   * @langEn Allows to map JSON schema definitions to an `objectType`, e.g. `'#/$defs/Link': Link`.
   * @langDe Erlaubt es JSON Schema Definitionen einem `objectType` zuzuweisen, z.B.
   *     `'#/$defs/Link': Link`.
   * @default {}
   * @since v3.4
   */
  Map<String, String> getObjectTypeRefs();

  /**
   * @langEn Allows to map JSON schema definitions to a `geometryType`, e.g.
   *     `https://geojson.org/schema/LineString.json': LINE_STRING`.
   * @langDe Erlaubt es JSON Schema Definitionen einem `geometryType` zuzuweisen, z.B.
   *     `'https://geojson.org/schema/LineString.json': LINE_STRING`.
   * @default {}
   * @since v3.4
   */
  Map<String, GeometryType> getGeometryTypeRefs();

  /**
   * @langEn Allows to map JSON schema definitions to a `refType`, e.g. `'#/$defs/Address':
   *     address`.
   * @langDe Erlaubt es JSON Schema Definitionen einem `refType` zuzuweisen, z.B.
   *     `'#/$defs/Address': address`.
   * @default {}
   * @since v3.4
   */
  Map<String, String> getRelationRefs();

  /**
   * @langEn Allows to choose a specific variant for oneOf/anyOf/allOf.
   * @langDe Erlaubt es eine bestimmte Variante für oneOf/anyOf/allOf auszuwählen.
   * @default 0
   * @since v3.4
   */
  Map<String, Integer> getCompositionIndexes();

  abstract class Builder extends ExtensionConfiguration.Builder {}

  @Override
  default Builder getBuilder() {
    return new ImmutableJsonSchemaConfiguration.Builder();
  }
}
