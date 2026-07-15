/*
 * Copyright 2026 interactive instruments GmbH
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
import java.util.stream.Collectors;

/**
 * Removes {@code internal} properties from a schema. Internal properties are read from the data
 * source and are part of the provider-side schemas (unlike scope exclusions, which are applied
 * there via {@link WithScope}), but they are not part of any public schema — apply this visitor
 * when deriving a schema that is exposed to clients (JSON Schema, OpenAPI definitions).
 */
public class WithoutInternal implements SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {

  @Override
  public FeatureSchema visit(
      FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {

    if (schema.isInternal() && !parents.isEmpty()) {
      return null;
    }

    Map<String, FeatureSchema> visitedPropertiesMap =
        visitedProperties.stream()
            .filter(Objects::nonNull)
            .map(
                featureSchema ->
                    new SimpleImmutableEntry<>(featureSchema.getFullPathAsString(), featureSchema))
            .collect(
                ImmutableMap.toImmutableMap(
                    Entry::getKey, Entry::getValue, (first, second) -> second));
    List<FeatureSchema> visitedConcat =
        schema.getConcat().stream()
            .map(concatSchema -> concatSchema.accept(this, parents))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    List<FeatureSchema> visitedCoalesce =
        schema.getCoalesce().stream()
            .map(coalesceSchema -> coalesceSchema.accept(this, parents))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    return new ImmutableFeatureSchema.Builder()
        .from(schema)
        .propertyMap(visitedPropertiesMap)
        .concat(visitedConcat)
        .coalesce(visitedCoalesce)
        .build();
  }
}
