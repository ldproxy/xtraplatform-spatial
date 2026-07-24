/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Stream;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface FeatureSchemaTransformer {

  FeatureSchema visit(
      FeatureSchema schema,
      List<FeatureSchema> parents,
      List<FeatureSchema> visitedProperties,
      List<PartialObjectSchema> visitedMergeProperties,
      List<FeatureSchema> visitedConcatProperties,
      List<FeatureSchema> visitedCoalesceProperties);

  default Map<String, FeatureSchema> asMap(Stream<FeatureSchema> visitedProperties) {
    return visitedProperties
        .filter(Objects::nonNull)
        .map(schema -> new SimpleImmutableEntry<>(schema.getName(), schema))
        .collect(
            ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue, (first, second) -> second));
  }
}
