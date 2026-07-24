/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaVisitorTopDown;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FeatureSchemaFlattener implements SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {

  private final String separator;
  private final String labelSeparator;
  private final String arraySuffix;

  public FeatureSchemaFlattener(String separator) {
    this.separator = separator;
    this.labelSeparator = " > ";
    this.arraySuffix = "[]";
  }

  @Override
  public FeatureSchema visit(
      FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {
    if (parents.isEmpty()) {
      Map<String, FeatureSchema> flatProperties =
          flattenProperties(visitedProperties, null, null).stream()
              .map(property -> new SimpleEntry<>(flatIdentifier(property), property))
              .collect(
                  ImmutableMap.toImmutableMap(
                      Entry::getKey, Entry::getValue, (first, second) -> second));

      return new ImmutableFeatureSchema.Builder().from(schema).propertyMap(flatProperties).build();
    }

    return new ImmutableFeatureSchema.Builder()
        .from(schema)
        .propertyMap(asMap(visitedProperties, FeatureSchema::getFullPathAsString))
        .build();
  }

  private List<FeatureSchema> flattenProperties(
      List<FeatureSchema> properties, String parentName, String parentLabel) {
    String prefix = Objects.nonNull(parentName) ? parentName + separator : "";
    String labelPrefix = Objects.nonNull(parentLabel) ? parentLabel + labelSeparator : "";

    return properties.stream()
        .filter(Objects::nonNull)
        .flatMap(
            property ->
                property.isObject()
                    ? flattenProperties(
                        property.getProperties(),
                        flatName(property, prefix),
                        flatLabel(property, labelPrefix))
                        .stream()
                    : Stream.of(flattenProperty(property, prefix, labelPrefix)))
        .collect(Collectors.toList());
  }

  private FeatureSchema flattenProperty(
      FeatureSchema property, String namePrefix, String labelPrefix) {
    return new ImmutableFeatureSchema.Builder()
        .from(property)
        .type(flatType(property))
        .valueType(Optional.empty())
        .name(flatName(property, namePrefix))
        .label(flatLabel(property, labelPrefix))
        .path(flatPath(property))
        .concat(List.of())
        .coalesce(List.of())
        .build();
  }

  private String flatName(FeatureSchema property, String prefix) {
    return prefix + property.getName() + (property.isArray() ? arraySuffix : "");
  }

  private List<String> flatPath(FeatureSchema property) {
    return property.getFullPath();
  }

  private String flatLabel(FeatureSchema property, String prefix) {
    return prefix + property.getLabel().orElse(property.getName());
  }

  private String flatIdentifier(FeatureSchema property) {
    return String.join(separator, property.getPath());
  }

  private Type flatType(FeatureSchema property) {
    if (property.getValueType().isPresent()) {
      return property.getValueType().get();
    }

    if (!property.getConcat().isEmpty()) {
      return property.getConcat().stream()
          .map(FeatureSchema::getValueType)
          .findFirst()
          .flatMap(Function.identity())
          .orElse(property.getType());
    }

    if (!property.getCoalesce().isEmpty()) {
      return property.getCoalesce().stream()
          .map(FeatureSchema::getType)
          .findFirst()
          .orElse(property.getType());
    }

    return property.getType();
  }
}
