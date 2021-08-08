/**
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class SchemaTransformerChain implements
    TransformerChain<FeatureSchema, FeaturePropertySchemaTransformer> {

  public static final String OBJECT_TYPE_WILDCARD = "*{objectType=";

  private final List<String> currentParentProperties;
  private final Map<String, List<FeaturePropertySchemaTransformer>> transformers;

  public SchemaTransformerChain(Map<String, List<PropertyTransformation>> allTransformations,
      SchemaMapping schemaMapping, boolean inCollection,
      BiFunction<String, String, String> flattenedPathProvider) {
    this.currentParentProperties = new ArrayList<>();
    this.transformers = allTransformations.entrySet().stream()
        .flatMap(entry -> {
          String propertyPath = entry.getKey();
          List<PropertyTransformation> transformation = entry.getValue();

          if (hasWildcard(propertyPath, OBJECT_TYPE_WILDCARD)) {
            return createSchemaTransformersForObjectType(propertyPath, schemaMapping,
                transformation, inCollection, flattenedPathProvider).entrySet().stream();
          }

          return Stream.of(new SimpleEntry<>(propertyPath,
              createSchemaTransformers(propertyPath, transformation, inCollection,
                  flattenedPathProvider)));
        })
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, (first, second) -> second));
  }

  @Nullable
  @Override
  public FeatureSchema transform(String path, FeatureSchema schema) {
    FeatureSchema transformed = schema;
    boolean ran = false;

    for (int i = currentParentProperties.size() - 1; i >= 0; i--) {
      String parentPath = currentParentProperties.get(i);

      if (!path.startsWith(parentPath)) {
        currentParentProperties.remove(i);
      } else {
        transformed = run(transformers, parentPath, path, schema);
        ran = true;
        if (Objects.isNull(transformed)) {
          return null;
        }
      }
    }

    if (!schema.isValue() && (currentParentProperties.isEmpty() || !Objects.equals(
        currentParentProperties.get(currentParentProperties.size() - 1), path))) {
      currentParentProperties.add(path);
    }

    if (!ran) {
      transformed = run(transformers, path, path, schema);
    }

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
      Map<String, List<FeaturePropertySchemaTransformer>> schemaTransformations, String keyPath,
      String propertyPath, FeatureSchema schema) {
    FeatureSchema transformed = schema;

    if (schemaTransformations.containsKey(keyPath) && !schemaTransformations.get(keyPath)
        .isEmpty()) {
      for (FeaturePropertySchemaTransformer schemaTransformer : schemaTransformations.get(
          keyPath)) {
        transformed = schemaTransformer.transform(propertyPath, transformed);
        if (Objects.isNull(transformed)) {
          return null;
        }
      }
    } else if (schemaTransformations.containsKey(PropertyTransformations.WILDCARD)
        && !schemaTransformations.get(
            PropertyTransformations.WILDCARD)
        .isEmpty()) {
      for (FeaturePropertySchemaTransformer schemaTransformer : schemaTransformations.getOrDefault(
          PropertyTransformations.WILDCARD,
          ImmutableList.of())) {
        transformed = schemaTransformer.transform(propertyPath, transformed);
        if (Objects.isNull(transformed)) {
          return null;
        }
      }
    }

    return transformed;
  }

  private List<FeaturePropertySchemaTransformer> createSchemaTransformers(String path,
      List<PropertyTransformation> propertyTransformations, boolean inCollection,
      BiFunction<String, String, String> flattenedPathProvider) {
    List<FeaturePropertySchemaTransformer> transformers = new ArrayList<>();

    //TODO: RENAME, REMOVE, FLATTEN are not chainable, so only add last ones
    propertyTransformations.forEach(propertyTransformation -> {
      propertyTransformation.getRename()
          .ifPresent(rename -> transformers
              .add(ImmutableFeaturePropertyTransformerRename.builder()
                  .propertyPath(path)
                  .parameter(rename)
                  .build()));

      propertyTransformation.getRemove()
          .ifPresent(remove -> transformers
              .add(ImmutableFeaturePropertyTransformerRemove.builder()
                  .propertyPath(path)
                  .parameter(remove)
                  .inCollection(inCollection)
                  .build()));

      propertyTransformation.getFlatten()
          .ifPresent(flatten -> transformers
              .add(ImmutableFeaturePropertyTransformerFlatten.builder()
                  .propertyPath(path)
                  .parameter(flatten)
                  .flattenedPathProvider(flattenedPathProvider)
                  .build()));

      //TODO
    /*mapping.getFlattenObjects()
        .ifPresent(flatten -> transformers
            .add(ImmutableFeaturePropertyTransformerFlatten.builder()
                .parameter(flatten)
                .include(INCLUDE.OBJECTS)
                .flattenedPathProvider(flattenedPathProvider)
                .build()));

    mapping.getFlattenArrays()
        .ifPresent(flatten -> transformers
            .add(ImmutableFeaturePropertyTransformerFlatten.builder()
                .parameter(flatten)
                .include(INCLUDE.ARRAYS)
                .flattenedPathProvider(flattenedPathProvider)
                .build()));*/
    });

    return transformers;
  }

  private Map<String, List<FeaturePropertySchemaTransformer>> createSchemaTransformersForObjectType(
      String transformationKey, SchemaMapping schemaMapping,
      List<PropertyTransformation> propertyTransformation, boolean inCollection,
      BiFunction<String, String, String> flattenedPathProvider) {
    return explodeWildcard(transformationKey, OBJECT_TYPE_WILDCARD, schemaMapping,
        this::matchesObjectType)
        .stream()
        .map(propertyPath -> new SimpleEntry<>(propertyPath,
            createSchemaTransformers(propertyPath, propertyTransformation, inCollection,
                flattenedPathProvider)))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private boolean matchesObjectType(FeatureSchema schema, String objectType) {
    return schema.getObjectType().isPresent()
        && Objects.equals(schema.getObjectType().get(), objectType);
  }
}
