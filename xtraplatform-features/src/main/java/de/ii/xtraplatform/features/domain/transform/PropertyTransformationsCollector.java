/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import static de.ii.xtraplatform.features.domain.transform.PropertyTransformations.WILDCARD;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaVisitorWithFinalizer;
import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PropertyTransformationsCollector
    implements SchemaVisitorWithFinalizer<
        FeatureSchema, Map<String, List<PropertyTransformation>>, PropertyTransformations> {

  private final PropertyTransformations additionalTransformations;
  private final boolean preferSchemaTransformations;

  public PropertyTransformationsCollector() {
    this(new LinkedHashMap<>());
  }

  public PropertyTransformationsCollector(
      Map<String, PropertyTransformation> additionalTransformations) {
    this(
        () ->
            additionalTransformations.entrySet().stream()
                .map(entry -> new SimpleEntry<>(entry.getKey(), ImmutableList.of(entry.getValue())))
                .collect(
                    ImmutableMap.toImmutableMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (first, second) ->
                            ImmutableList.<PropertyTransformation>builder()
                                .addAll(first)
                                .addAll(second)
                                .build())),
        true);
  }

  public PropertyTransformationsCollector(PropertyTransformations additionalTransformations) {
    this(additionalTransformations, false);
  }

  public PropertyTransformationsCollector(
      PropertyTransformations additionalTransformations, boolean preferSchemaTransformations) {
    this.additionalTransformations = additionalTransformations;
    this.preferSchemaTransformations = preferSchemaTransformations;
  }

  @Override
  public Map<String, List<PropertyTransformation>> visit(
      FeatureSchema schema,
      List<FeatureSchema> parents,
      List<Map<String, List<PropertyTransformation>>> visitedProperties) {
    return java.util.stream.Stream.concat(
            schema.getTransformations().isEmpty()
                ? java.util.stream.Stream.empty()
                : java.util.stream.Stream.of(
                    Map.entry(
                        schema.getFullPath().isEmpty()
                            ? WILDCARD
                            : String.join(".", schema.getFullPath()),
                        schema.getTransformations())),
            visitedProperties.stream().flatMap(m -> m.entrySet().stream()))
        .collect(
            ImmutableMap.toImmutableMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (first, second) ->
                    ImmutableList.<PropertyTransformation>builder()
                        .addAll(first)
                        .addAll(second)
                        .build()));
  }

  @Override
  public PropertyTransformations finalizeVisit(
      FeatureSchema schema, Map<String, List<PropertyTransformation>> transformations) {
    PropertyTransformations schemaTransformations = () -> transformations;
    return preferSchemaTransformations
        ? schemaTransformations.mergeInto(additionalTransformations)
        : additionalTransformations.mergeInto(schemaTransformations);
  }
}
