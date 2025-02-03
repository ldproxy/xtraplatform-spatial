/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.features.domain.SchemaVisitorTopDown;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class SchemaTransformerChain
    implements TransformerChain<FeatureSchema, FeaturePropertySchemaTransformer>,
        SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {

  private final List<String> currentParentProperties;
  private final Map<String, List<FeaturePropertySchemaTransformer>> transformers;

  public SchemaTransformerChain(
      Map<String, List<PropertyTransformation>> allTransformations,
      SchemaMapping schemaMapping,
      boolean inCollection) {
    this.currentParentProperties = new ArrayList<>();
    this.transformers =
        allTransformations.entrySet().stream()
            .flatMap(
                entry -> {
                  String propertyPath = entry.getKey();
                  List<PropertyTransformation> transformation = entry.getValue();

                  if (hasWildcard(propertyPath)) {
                    List<String> propertyPaths = explodeWildcard(propertyPath, schemaMapping);
                    return createSchemaTransformersForPaths(
                        propertyPaths, transformation, inCollection)
                        .entrySet()
                        .stream();
                  }

                  return Stream.of(
                      new SimpleEntry<>(
                          propertyPath,
                          createSchemaTransformers(propertyPath, transformation, inCollection)));
                })
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (first, second) ->
                        new ImmutableList.Builder<FeaturePropertySchemaTransformer>()
                            .addAll(first)
                            .addAll(second)
                            .build()));
  }

  @Override
  public FeatureSchema visit(
      FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {
    return transform(
        schema.getFullPathAsString(),
        new ImmutableFeatureSchema.Builder()
            .from(schema)
            .propertyMap(asMap(visitedProperties, FeatureSchema::getFullPathAsString))
            .build());
  }

  @Nullable
  @Override
  public FeatureSchema transform(String path, FeatureSchema schema) {
    FeatureSchema transformed = schema;

    for (int i = currentParentProperties.size() - 1; i >= 0; i--) {
      String parentPath = currentParentProperties.get(i);

      if (!path.startsWith(parentPath)) {
        currentParentProperties.remove(i);
      } else if (transformers.containsKey(parentPath)) {
        transformed = run(transformers, parentPath, path, schema);
        if (Objects.isNull(transformed)) {
          return null;
        }
      }
    }

    if (!schema.isValue()
        && (currentParentProperties.isEmpty()
            || !Objects.equals(
                currentParentProperties.get(currentParentProperties.size() - 1), path))) {
      currentParentProperties.add(path);
    }

    transformed = run(transformers, path, path, schema);

    return transformed;
  }

  @Override
  public boolean has(String path) {
    return transformers.containsKey(path);
  }

  @Override
  public List<FeaturePropertySchemaTransformer> get(String path) {
    return transformers.get(path);
  }

  private FeatureSchema run(
      Map<String, List<FeaturePropertySchemaTransformer>> schemaTransformations,
      String keyPath,
      String propertyPath,
      FeatureSchema schema) {
    FeatureSchema transformed = schema;

    if (schemaTransformations.containsKey(keyPath)
        && !schemaTransformations.get(keyPath).isEmpty()) {
      for (FeaturePropertySchemaTransformer schemaTransformer :
          schemaTransformations.get(keyPath)) {
        transformed = schemaTransformer.transform(propertyPath, transformed);
        if (Objects.isNull(transformed)) {
          return null;
        }
      }
    } else if (schemaTransformations.containsKey(PropertyTransformations.WILDCARD)
        && !schemaTransformations.get(PropertyTransformations.WILDCARD).isEmpty()) {
      for (FeaturePropertySchemaTransformer schemaTransformer :
          schemaTransformations.getOrDefault(
              PropertyTransformations.WILDCARD, ImmutableList.of())) {
        transformed = schemaTransformer.transform(propertyPath, transformed);
        if (Objects.isNull(transformed)) {
          return null;
        }
      }
    }

    return transformed;
  }

  private List<FeaturePropertySchemaTransformer> createSchemaTransformers(
      String path, List<PropertyTransformation> propertyTransformations, boolean inCollection) {
    List<FeaturePropertySchemaTransformer> transformers = new ArrayList<>();

    propertyTransformations.forEach(
        propertyTransformation -> {
          propertyTransformation
              .getRename()
              .ifPresent(
                  rename ->
                      transformers.add(
                          ImmutableFeaturePropertyTransformerRename.builder()
                              .propertyPath(path)
                              .parameter(rename)
                              .build()));

          propertyTransformation
              .getRenamePathOnly()
              .ifPresent(
                  rename ->
                      transformers.add(
                          ImmutableFeaturePropertyTransformerRename.builder()
                              .propertyPath(path)
                              .parameter(rename)
                              .pathOnly(true)
                              .build()));

          propertyTransformation
              .getRemove()
              .ifPresent(
                  remove ->
                      transformers.add(
                          ImmutableFeaturePropertyTransformerRemove.builder()
                              .propertyPath(path)
                              .parameter(remove)
                              .inCollection(inCollection)
                              .build()));

          propertyTransformation
              .getCodelist()
              .ifPresent(
                  codelist ->
                      transformers.add(
                          ImmutableFeaturePropertyTransformerValueType.builder()
                              .propertyPath(path)
                              .parameter(SchemaBase.Type.STRING.name())
                              .build()));
        });

    return transformers;
  }

  private Map<String, List<FeaturePropertySchemaTransformer>> createSchemaTransformersForPaths(
      List<String> propertyPaths,
      List<PropertyTransformation> propertyTransformation,
      boolean inCollection) {
    return propertyPaths.stream()
        .map(
            propertyPath ->
                new SimpleEntry<>(
                    propertyPath,
                    createSchemaTransformers(propertyPath, propertyTransformation, inCollection)))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
