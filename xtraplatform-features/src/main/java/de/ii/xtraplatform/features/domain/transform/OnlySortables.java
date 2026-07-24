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
import de.ii.xtraplatform.features.domain.SchemaVisitorTopDown;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnlySortables implements SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OnlySortables.class);

  private final boolean wildcard;
  private final List<String> included;
  private final List<String> excluded;
  private final String pathSeparator;
  private final Predicate<String> excludePathMatcher;

  public OnlySortables(
      List<String> included,
      List<String> excluded,
      String pathSeparator,
      Predicate<String> excludePathMatcher) {
    this.included = included;
    this.excluded = excluded;
    this.wildcard = included.contains("*");
    this.pathSeparator = pathSeparator;
    this.excludePathMatcher = excludePathMatcher;
  }

  @Override
  public FeatureSchema visit(
      FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {
    if (parents.isEmpty()) {
      PropertyTransformations propertyTransformations =
          schema.accept(new PropertyTransformationsCollector());
      SchemaTransformerChain schemaTransformations =
          propertyTransformations.getSchemaTransformations(null, false);

      return schema.accept(schemaTransformations).accept(new OnlySortablesIncluder());
    }
    return schema;
  }

  class OnlySortablesIncluder implements SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {

    @Override
    @SuppressWarnings("PMD.CyclomaticComplexity")
    public FeatureSchema visit(
        FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {

      if (parents.size() > 1) {
        // only direct properties can be a sortable
        return null;
      }

      if (parents.stream().anyMatch(s -> excludePathMatcher.test(s.getSourcePath().orElse("")))) {
        // if the path is excluded, no property can be a sortable
        return null;
      }

      if (schema.sortable()) {
        String path = OnlyQueryables.cleanupPaths(schema).getFullPathAsString(pathSeparator);
        // ignore property, if it is not included (by default or explicitly) or if it is excluded
        if ((!wildcard && !included.contains(path)) || excluded.contains(path)) {
          return null;
        }
        if (excludePathMatcher.test(schema.getSourcePath().orElse(""))) {
          return null;
        }

        // TODO: In the next major release move to FeatureSchema.sortable() and exclude
        //       incompatible properties.
        if (!isCompatible(schema) && shouldSkipIncompatibleSortable(schema)) {
          return null;
        }
      } else if (!schema.isFeature()) {
        return null;
      }

      Map<String, FeatureSchema> visitedPropertiesMap =
          visitedProperties.stream()
              .filter(Objects::nonNull)
              .map(
                  featureSchema ->
                      new SimpleImmutableEntry<>(
                          featureSchema.getFullPathAsString(pathSeparator), featureSchema))
              .collect(
                  ImmutableMap.toImmutableMap(
                      Entry::getKey, Entry::getValue, (first, second) -> second));

      List<FeatureSchema> visitedConcat =
          schema.getConcat().stream()
              .map(concatSchema -> concatSchema.accept(this, parents))
              .collect(Collectors.toList());

      return new ImmutableFeatureSchema.Builder()
          .from(schema)
          .propertyMap(visitedPropertiesMap)
          .concat(visitedConcat)
          .build();
    }

    private void logIncompatibleSortable(FeatureSchema schema) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "Property '{}' has a value transformation or is a constant value. The property should not be used as a sortable as sorting by the values may not work as expected.",
            schema.getFullPathAsString(pathSeparator));
      }
    }

    private boolean shouldSkipIncompatibleSortable(FeatureSchema schema) {
      if (!wildcard) {
        logIncompatibleSortable(schema);
        return false;
      }
      return true;
    }
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
}
