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
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaVisitorTopDown;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnlyQueryables implements SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OnlyQueryables.class);

  private final boolean wildcard;
  private final List<String> included;
  private final List<String> excluded;
  private final String pathSeparator;
  private final Predicate<String> excludePathMatcher;
  private final boolean cleanupKeys;

  public OnlyQueryables(
      List<String> included,
      List<String> excluded,
      String pathSeparator,
      Predicate<String> excludePathMatcher,
      boolean cleanupKeys) {
    this.included = included;
    this.excluded = excluded;
    this.wildcard = included.contains("*");
    this.pathSeparator = pathSeparator;
    this.excludePathMatcher = excludePathMatcher;
    this.cleanupKeys = cleanupKeys;
  }

  @Override
  public FeatureSchema visit(
      FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {
    if (parents.isEmpty()) {
      PropertyTransformations propertyTransformations =
          schema.accept(new PropertyTransformationsCollector());
      SchemaTransformerChain schemaTransformations =
          propertyTransformations.getSchemaTransformations(null, false);

      return schema.accept(schemaTransformations).accept(new OnlyQueryablesIncluder());
    }

    return schema;
  }

  class OnlyQueryablesIncluder implements SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {
    @Override
    @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
    public FeatureSchema visit(
        FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {

      if (parents.stream().anyMatch(this::isParentExcluded)) {
        return null;
      }

      if (schema.queryable()) {
        String path = cleanupPaths(schema).getFullPathAsString(pathSeparator);
        // ignore property, if it is not included (by default or explicitly) or if it is excluded
        if ((!wildcard && !included.contains(path)) || excluded.contains(path)) {
          return null;
        }
        if (excludePathMatcher.test(schema.getSourcePath().orElse(""))) {
          return null;
        }

        // TODO: In the next major release move to FeatureSchema.queryable() and exclude
        //       incompatible properties.
        if (!isCompatible(schema) && shouldSkipIncompatibleQueryable(schema)) {
          return null;
        }
      } else if (!schema.isObject()
          || (!parents.isEmpty() && visitedProperties.stream().noneMatch(Objects::nonNull))) {
        return null;
      }

      int requiredCount =
          !cleanupKeys || schema.getConcat().isEmpty() ? 1 : schema.getConcat().size();
      Map<String, Integer> counts = new HashMap<>();

      Map<String, FeatureSchema> visitedPropertiesMap =
          visitedProperties.stream()
              .filter(Objects::nonNull)
              .map(this::cleanupPathsIfDesired)
              .map(
                  property ->
                      new SimpleImmutableEntry<>(
                          property.getFullPathAsString(pathSeparator), property))
              .collect(
                  ImmutableMap.toImmutableMap(
                      Entry::getKey,
                      Entry::getValue,
                      (first, second) -> {
                        counts.put(
                            first.getFullPathAsString(pathSeparator),
                            counts.getOrDefault(first.getFullPathAsString(pathSeparator), 1) + 1);
                        return second;
                      }));

      // ignore properties that are not included in all concat variants
      Map<String, FeatureSchema> filteredPropertiesMap =
          requiredCount == 1
              ? visitedPropertiesMap
              : visitedPropertiesMap.entrySet().stream()
                  .filter(entry -> counts.getOrDefault(entry.getKey(), 1) == requiredCount)
                  .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

      return new Builder()
          .from(adjustType(parents, schema))
          .propertyMap(filteredPropertiesMap)
          .concat(List.of())
          .build();
    }

    private boolean isCompatible(FeatureSchema schema) {
      return schema.getConstantValue().isEmpty()
          && schema.getTransformations().stream()
              .allMatch(
                  t ->
                      t.getNullify().isEmpty()
                          && t.getStringFormat().isEmpty()
                          && t.getDateFormat().isEmpty()
                          && t.getMap().isEmpty()
                          && t.getCodelist().isEmpty());
    }

    private FeatureSchema cleanupPathsIfDesired(FeatureSchema property) {
      if (cleanupKeys) {
        return cleanupPaths(property);
      }
      return property;
    }

    private FeatureSchema adjustType(List<FeatureSchema> parents, FeatureSchema property) {
      if (!property.queryable()) {
        // not a queryable, we have an object that has embedded queryables
        return property;
      }

      if (parents.stream().noneMatch(SchemaBase::isArray)) {
        // nothing to do, the property is not embedded in an array
        return property;
      }

      if (property.isArray()) {
        // nothing to do, already an array
        return property;
      }

      return new ImmutableFeatureSchema.Builder()
          .from(property)
          .type(Type.VALUE_ARRAY)
          .valueType(property.getType())
          .build();
    }

    private boolean isParentExcluded(FeatureSchema parent) {
      // if the parent has multiple source paths or its target path is excluded, no property can
      // be a queryable
      return (parent.isMultiSource() && !parent.isFeature())
          || excludePathMatcher.test(parent.getSourcePath().orElse(""));
    }

    private void logIncompatibleQueryable(FeatureSchema schema) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "Property '{}' has a value transformation or is a constant value. The property should not be used as a queryable as filtering on the values may not work as expected.",
            schema.getFullPathAsString(pathSeparator));
      }
    }

    private boolean shouldSkipIncompatibleQueryable(FeatureSchema schema) {
      if (!wildcard) {
        logIncompatibleQueryable(schema);
        return false;
      }
      return true;
    }
  }

  static FeatureSchema cleanupPaths(FeatureSchema property) {
    if (property.getPath().stream().anyMatch(elem -> elem.matches("^([0-9]+)_.*"))
        || property.getParentPath().stream().anyMatch(elem -> elem.matches("^([0-9]+)_.*"))) {
      return new ImmutableFeatureSchema.Builder()
          .from(property)
          .path(
              property.getPath().stream()
                  .map(elem -> elem.replaceAll("^([0-9]+)_", ""))
                  .collect(Collectors.toList()))
          .parentPath(
              property.getParentPath().stream()
                  .map(elem -> elem.replaceAll("^([0-9]+)_", ""))
                  .collect(Collectors.toList()))
          .build();
    }
    return property;
  }
}
