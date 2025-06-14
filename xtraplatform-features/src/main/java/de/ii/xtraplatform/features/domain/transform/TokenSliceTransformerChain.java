/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.codelists.domain.Codelist;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.features.domain.SchemaVisitorTopDown;
import java.time.ZoneId;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenSliceTransformerChain
    implements TransformerChain<List<Object>, FeaturePropertyTokenSliceTransformer>,
        SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TokenSliceTransformerChain.class);
  private static final Splitter PATH_SPLITTER = Splitter.on('.');

  private final SchemaMapping schemaMapping;
  private final Map<String, List<FeaturePropertyTokenSliceTransformer>> transformers;

  public TokenSliceTransformerChain(
      Map<String, List<PropertyTransformation>> allTransformations,
      SchemaMapping schemaMapping,
      Function<String, String> substitutionLookup,
      Map<String, Codelist> codelists,
      ZoneId defaultTimeZone) {
    this.schemaMapping = schemaMapping;
    this.transformers =
        allTransformations.entrySet().stream()
            .flatMap(
                entry -> {
                  String propertyPath = entry.getKey();
                  List<PropertyTransformation> transformation = entry.getValue();

                  if (hasWildcard(propertyPath)) {
                    List<String> propertyPaths = explodeWildcard(propertyPath, schemaMapping);

                    return createSliceTransformersForPaths(
                        propertyPaths,
                        transformation,
                        substitutionLookup,
                        codelists,
                        defaultTimeZone)
                        .entrySet()
                        .stream();
                  }

                  if (!Objects.equals(propertyPath, PropertyTransformations.WILDCARD)
                      && Objects.nonNull(schemaMapping)
                      && schemaMapping.getSchemasByTargetPath().keySet().stream()
                          .noneMatch(
                              path -> Objects.equals(propertyPath, String.join(".", path)))) {
                    return Stream.empty();
                  }

                  return Stream.of(
                      new SimpleEntry<>(
                          propertyPath,
                          createSliceTransformers(
                              propertyPath,
                              transformation,
                              substitutionLookup,
                              codelists,
                              defaultTimeZone)));
                })
            .collect(
                ImmutableMap.toImmutableMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (first, second) ->
                        new ImmutableList.Builder<FeaturePropertyTokenSliceTransformer>()
                            .addAll(first)
                            .addAll(second)
                            .build()));
  }

  @Override
  public FeatureSchema visit(
      FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {
    return transformSchema(
        transformers,
        schema.getFullPathAsString(),
        new ImmutableFeatureSchema.Builder()
            .from(schema)
            .propertyMap(asMap(visitedProperties, FeatureSchema::getFullPathAsString))
            .build());
  }

  public Map<String, String> transform(FeatureEventBuffer buffer) {
    Map<String, String> applied = new HashMap<>();

    Map<String, List<FeaturePropertyTokenSliceTransformer>> filteredTransformers;
    Optional<FeaturePropertyTokenSliceTransformer> flatten;

    // flatten is a special case, as it needs to be applied after all other transformers
    if (transformers.containsKey(PropertyTransformations.WILDCARD)
        && transformers.get(PropertyTransformations.WILDCARD).stream()
            .anyMatch(transformer -> transformer instanceof FeaturePropertyTransformerFlatten)) {
      filteredTransformers = new LinkedHashMap<>(transformers);
      filteredTransformers.put(
          PropertyTransformations.WILDCARD,
          transformers.get(PropertyTransformations.WILDCARD).stream()
              .filter(transformer -> !(transformer instanceof FeaturePropertyTransformerFlatten))
              .collect(Collectors.toList()));
      flatten =
          transformers.get(PropertyTransformations.WILDCARD).stream()
              .filter(transformer -> transformer instanceof FeaturePropertyTransformerFlatten)
              .findFirst();
    } else {
      filteredTransformers = transformers;
      flatten = Optional.empty();
    }

    filteredTransformers.keySet().stream()
        .filter(path -> !filteredTransformers.get(path).isEmpty())
        .forEach(
            path -> {
              schemaMapping
                  .getPositionsForTargetPath(
                      Objects.equals(path, PropertyTransformations.WILDCARD)
                          ? List.of()
                          : Splitter.on('.').splitToList(path))
                  .forEach(
                      pos -> {
                        List<Object> slice = buffer.getSlice(pos);

                        if (LOGGER.isTraceEnabled()) {
                          LOGGER.trace(
                              "Token slice before transformations [{}]:\n{}\n",
                              path,
                              FeatureEventBuffer.sliceToString(slice));
                        }

                        List<Object> transformed = run(filteredTransformers, path, path, slice);

                        boolean replaced = buffer.replaceSlice(pos, transformed);

                        String transformerNames =
                            filteredTransformers.get(path).stream()
                                .map(FeaturePropertyTransformer::getType)
                                .collect(Collectors.joining(","));

                        applied.put(path, transformerNames);

                        if (LOGGER.isTraceEnabled()) {
                          if (replaced) {
                            LOGGER.trace(
                                "Token slice after transformations [{}] ({}):\n{}\n",
                                path,
                                transformerNames,
                                FeatureEventBuffer.sliceToString(transformed));
                          } else {
                            LOGGER.trace(
                                "Token slice unchanged after transformations [{}] ({})",
                                path,
                                transformerNames);
                          }
                        }
                      });
            });

    // flatten is a special case, as it needs to be applied after all other transformers
    if (flatten.isPresent()) {
      String path = PropertyTransformations.WILDCARD;
      schemaMapping
          .getPositionsForTargetPath(List.of())
          .forEach(
              pos -> {
                List<Object> slice = buffer.getSlice(pos);

                List<Object> transformed =
                    run(Map.of(path, List.of(flatten.get())), path, path, slice);

                buffer.replaceSlice(pos, transformed);

                String transformerNames =
                    transformers.get(path).stream()
                        .map(FeaturePropertyTransformer::getType)
                        .collect(Collectors.joining(","));

                applied.put(path, transformerNames);
              });
    }

    return applied;
  }

  @Nullable
  @Override
  public List<Object> transform(String path, List<Object> slice) {
    List<Object> transformed = slice;

    transformed = run(transformers, path, path, slice);

    return transformed;
  }

  @Override
  public boolean has(String path) {
    return transformers.containsKey(path);
  }

  @Override
  public List<FeaturePropertyTokenSliceTransformer> get(String path) {
    return transformers.get(path);
  }

  private List<Object> run(
      Map<String, List<FeaturePropertyTokenSliceTransformer>> sliceTransformers,
      String keyPath,
      String propertyPath,
      List<Object> slice) {
    List<Object> transformed = slice;

    if (sliceTransformers.containsKey(keyPath) && !sliceTransformers.get(keyPath).isEmpty()) {
      for (FeaturePropertyTokenSliceTransformer sliceTransformer : sliceTransformers.get(keyPath)) {
        transformed = sliceTransformer.transform(propertyPath, transformed);
      }
    } else if (sliceTransformers.containsKey(PropertyTransformations.WILDCARD)
        && !sliceTransformers.get(PropertyTransformations.WILDCARD).isEmpty()) {
      for (FeaturePropertyTokenSliceTransformer sliceTransformer :
          sliceTransformers.getOrDefault(PropertyTransformations.WILDCARD, ImmutableList.of())) {
        transformed = sliceTransformer.transform(propertyPath, transformed);
      }
    }

    return transformed;
  }

  private FeatureSchema transformSchema(
      Map<String, List<FeaturePropertyTokenSliceTransformer>> sliceTransformers,
      String path,
      FeatureSchema schema) {
    FeatureSchema transformed = schema;

    if (sliceTransformers.containsKey(path) && !sliceTransformers.get(path).isEmpty()) {
      for (FeaturePropertyTokenSliceTransformer sliceTransformer : sliceTransformers.get(path)) {
        transformed = sliceTransformer.transformSchema(transformed);
      }
    } else if (sliceTransformers.containsKey(PropertyTransformations.WILDCARD)
        && !sliceTransformers.get(PropertyTransformations.WILDCARD).isEmpty()) {
      for (FeaturePropertyTokenSliceTransformer sliceTransformer :
          sliceTransformers.getOrDefault(PropertyTransformations.WILDCARD, ImmutableList.of())) {
        transformed = sliceTransformer.transformSchema(transformed);
      }
    }

    return transformed;
  }

  private List<FeaturePropertyTokenSliceTransformer> createSliceTransformers(
      String path,
      List<PropertyTransformation> propertyTransformations,
      Function<String, String> substitutionLookup,
      Map<String, Codelist> codelists,
      ZoneId defaultTimeZone) {
    List<FeaturePropertyTokenSliceTransformer> transformers = new ArrayList<>();

    propertyTransformations.forEach(
        propertyTransformation -> {
          propertyTransformation
              .getObjectRemoveSelect()
              .ifPresent(
                  selected ->
                      transformers.add(
                          ImmutableFeaturePropertyTransformerObjectRemoveSelect.builder()
                              .propertyPath(path)
                              .parameter(selected)
                              .build()));

          propertyTransformation
              .getObjectReduceFormat()
              .ifPresent(
                  stringFormat ->
                      transformers.add(
                          ImmutableFeaturePropertyTransformerObjectReduceFormat.builder()
                              .propertyPath(path)
                              .parameter(stringFormat)
                              .substitutionLookup(substitutionLookup)
                              .build()));

          propertyTransformation
              .getObjectReduceSelect()
              .ifPresent(
                  selected ->
                      transformers.add(
                          ImmutableFeaturePropertyTransformerObjectReduceSelect.builder()
                              .propertyPath(path)
                              .parameter(selected)
                              .build()));

          if (!propertyTransformation.getObjectMapFormat().isEmpty()) {
            transformers.add(
                ImmutableFeaturePropertyTransformerObjectMapFormat.builder()
                    .propertyPath(path)
                    .parameter("")
                    .substitutionLookup(substitutionLookup)
                    .mapping(propertyTransformation.getObjectMapFormat())
                    .build());
          }

          if (!propertyTransformation.getObjectMapDuplicate().isEmpty()) {
            transformers.add(
                ImmutableFeaturePropertyTransformerObjectMapDuplicate.builder()
                    .propertyPath(path)
                    .parameter("")
                    .mapping(propertyTransformation.getObjectMapDuplicate())
                    .build());
          }

          if (!propertyTransformation.getObjectAddConstants().isEmpty()) {
            transformers.add(
                ImmutableFeaturePropertyTransformerObjectAddConstants.builder()
                    .propertyPath(path)
                    .parameter("")
                    .mapping(propertyTransformation.getObjectAddConstants())
                    .build());
          }

          propertyTransformation
              .getArrayReduceFormat()
              .ifPresent(
                  stringFormat ->
                      transformers.add(
                          ImmutableFeaturePropertyTransformerArrayReduceFormat.builder()
                              .propertyPath(path)
                              .parameter(stringFormat)
                              .substitutionLookup(substitutionLookup)
                              .build()));

          propertyTransformation
              .getCoalesce()
              .ifPresent(
                  isObject -> {
                    List<List<FeaturePropertyValueTransformer>> valueTransformers =
                        getValueTransformers(
                            path, false, isObject, substitutionLookup, codelists, defaultTimeZone);

                    transformers.add(
                        ImmutableFeaturePropertyTransformerCoalesce.builder()
                            .propertyPath(path)
                            .parameter("")
                            .isObject(isObject)
                            .valueTransformers(valueTransformers)
                            .build());
                  });

          propertyTransformation
              .getConcat()
              .ifPresent(
                  isObject -> {
                    List<List<FeaturePropertyValueTransformer>> valueTransformers =
                        getValueTransformers(
                            path, true, isObject, substitutionLookup, codelists, defaultTimeZone);

                    transformers.add(
                        ImmutableFeaturePropertyTransformerConcat.builder()
                            .propertyPath(path)
                            .parameter("")
                            .isObject(isObject)
                            .valueTransformers(valueTransformers)
                            .build());
                  });

          propertyTransformation
              .getFlatten()
              .ifPresent(
                  flatten ->
                      transformers.add(
                          ImmutableFeaturePropertyTransformerFlatten.builder()
                              .propertyPath(path)
                              .parameter(flatten)
                              .build()));

          propertyTransformation
              .getWrap()
              .ifPresent(
                  type ->
                      transformers.add(
                          ImmutableFeaturePropertyTransformerWrap.builder()
                              .propertyPath(path)
                              .parameter("")
                              .wrapper(type)
                              .build()));
        });

    return transformers;
  }

  private Map<String, List<FeaturePropertyTokenSliceTransformer>> createSliceTransformersForPaths(
      List<String> propertyPaths,
      List<PropertyTransformation> propertyTransformation,
      Function<String, String> substitutionLookup,
      Map<String, Codelist> codelists,
      ZoneId defaultTimeZone) {
    return propertyPaths.stream()
        .map(
            propertyPath ->
                new SimpleEntry<>(
                    propertyPath,
                    createSliceTransformers(
                        propertyPath,
                        propertyTransformation,
                        substitutionLookup,
                        codelists,
                        defaultTimeZone)))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private List<List<FeaturePropertyValueTransformer>> getValueTransformers(
      String path,
      boolean isConcat,
      boolean isObject,
      Function<String, String> substitutionLookup,
      Map<String, Codelist> codelists,
      ZoneId defaultTimeZone) {
    if (isObject || Objects.isNull(schemaMapping)) {
      return List.of();
    }

    return schemaMapping.getSchemasForTargetPath(PATH_SPLITTER.splitToList(path)).stream()
        .flatMap(schema -> isConcat ? schema.getConcat().stream() : schema.getCoalesce().stream())
        .<PropertyTransformations>map(schema -> () -> Map.of(path, schema.getTransformations()))
        .map(
            transformations ->
                transformations
                    .getValueTransformations(
                        schemaMapping, codelists, defaultTimeZone, substitutionLookup)
                    .get(path))
        .collect(Collectors.toList());
  }
}
