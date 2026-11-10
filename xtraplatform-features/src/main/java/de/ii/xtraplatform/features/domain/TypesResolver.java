/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public interface TypesResolver extends FeatureSchemaTransformer {

  default int maxRounds() {
    return 1;
  }

  default Optional<String> maxRoundsWarning() {
    return Optional.empty();
  }

  boolean needsResolving(
      FeatureSchema property, boolean isFeature, boolean isInConcat, boolean isInCoalesce);

  default boolean needsResolving(PartialObjectSchema partial) {
    return false;
  }

  FeatureSchema resolve(FeatureSchema property, List<FeatureSchema> parents);

  default boolean needsResolving(Map<String, FeatureSchema> types) {
    return types.values().stream().anyMatch(type -> needsResolving(type, true, false, false))
        || types.values().stream()
            .flatMap(type -> type.getAllNestedProperties().stream())
            .anyMatch(property -> needsResolving(property, false, false, false))
        || types.values().stream()
            .flatMap(type -> type.getAllNestedPartials().stream())
            .anyMatch(this::needsResolving)
        || types.values().stream()
            .flatMap(type -> type.getAllNestedConcatProperties().stream())
            .anyMatch(property -> needsResolving(property, false, true, false))
        || types.values().stream()
            .flatMap(type -> type.getAllNestedCoalesceProperties().stream())
            .anyMatch(property -> needsResolving(property, false, false, true));
  }

  default Map<String, FeatureSchema> resolve(Map<String, FeatureSchema> types) {
    Map<String, FeatureSchema> resolvedTypes = new LinkedHashMap<>();

    types.forEach(
        (key, value) -> {
          FeatureSchema resolved = value.accept(this, List.of());

          if (Objects.nonNull(resolved)) {
            resolvedTypes.put(key, resolved);
          }
        });

    return resolvedTypes;
  }

  @Override
  default FeatureSchema visit(
      FeatureSchema schema,
      List<FeatureSchema> parents,
      List<FeatureSchema> visitedProperties,
      List<PartialObjectSchema> visitedMergeProperties,
      List<FeatureSchema> visitedConcatProperties,
      List<FeatureSchema> visitedCoalesceProperties) {
    ImmutableFeatureSchema visited =
        new Builder()
            .from(schema)
            .propertyMap(asMap(visitedProperties.stream()))
            .merge(visitedMergeProperties)
            .concat(visitedConcatProperties)
            .coalesce(visitedCoalesceProperties)
            .build();

    if (needsResolving(
        visited,
        parents.isEmpty(),
        !parents.isEmpty() && parents.get(parents.size() - 1).isConcatElement(),
        !parents.isEmpty() && parents.get(parents.size() - 1).isCoalesceElement())) {
      return resolve(visited, parents);
    }

    return visited;
  }
}
