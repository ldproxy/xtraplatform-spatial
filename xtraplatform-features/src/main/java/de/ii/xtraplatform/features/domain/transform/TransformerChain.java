/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public interface TransformerChain<T, U> {

  String WILDCARD_PREFIX = "*{";
  String TYPE_WILDCARD = wildcard("type");
  String VALUE_TYPE_WILDCARD = wildcard("valueType");
  String OBJECT_TYPE_WILDCARD = wildcard("objectType");

  static String wildcard(String parameter) {
    return WILDCARD_PREFIX + parameter + "=";
  }

  @Nullable
  T transform(String path, T schema);

  boolean has(String path);

  List<U> get(String path);

  default boolean hasWildcard(String propertyPath, String wildcardParameter) {
    return propertyPath.startsWith(wildcardParameter)
        && propertyPath.length() > wildcardParameter.length() + 1;
  }

  default boolean hasWildcard(String propertyPath) {
    return propertyPath.startsWith(WILDCARD_PREFIX);
  }

  default String extractWildcardParameter(String propertyPath, String wildcardParameter) {
    return propertyPath.substring(wildcardParameter.length(), propertyPath.length() - 1);
  }

  default List<String> explodeWildcard(String transformationKey, SchemaMapping schemaMapping) {
    if (Objects.isNull(schemaMapping)) {
      return List.of();
    }
    if (hasWildcard(transformationKey, TYPE_WILDCARD)) {
      return explodeWildcard(
          transformationKey, TYPE_WILDCARD, schemaMapping, TransformerChain::matchesType);
    } else if (hasWildcard(transformationKey, VALUE_TYPE_WILDCARD)) {
      return explodeWildcard(
          transformationKey,
          VALUE_TYPE_WILDCARD,
          schemaMapping,
          TransformerChain::matchesValueType);
    } else if (hasWildcard(transformationKey, OBJECT_TYPE_WILDCARD)) {
      return explodeWildcard(
          transformationKey,
          OBJECT_TYPE_WILDCARD,
          schemaMapping,
          TransformerChain::matchesObjectType);
    }

    return List.of();
  }

  default List<String> explodeWildcard(
      String transformationKey,
      String wildcardPattern,
      SchemaMapping schemaMapping,
      BiPredicate<FeatureSchema, String> filter) {
    return schemaMapping.getSchemasByTargetPath().entrySet().stream()
        .filter(
            entry ->
                entry.getValue().stream()
                    .anyMatch(
                        schema ->
                            filter.test(
                                schema,
                                extractWildcardParameter(transformationKey, wildcardPattern))))
        .map(entry -> String.join(".", entry.getKey()))
        .collect(Collectors.toList());
  }

  static boolean matchesType(FeatureSchema schema, String type) {
    return schema.isValue() && Objects.equals(schema.getType(), Type.valueOf(type));
  }

  static boolean matchesValueType(FeatureSchema schema, String valueType) {
    return schema.isValue()
        && !schema.isSpatial()
        && Objects.equals(schema.getValueType().orElse(schema.getType()), Type.valueOf(valueType));
  }

  static boolean matchesObjectType(FeatureSchema schema, String objectType) {
    return schema.getObjectType().isPresent()
        && Objects.equals(schema.getObjectType().get(), objectType);
  }
}
