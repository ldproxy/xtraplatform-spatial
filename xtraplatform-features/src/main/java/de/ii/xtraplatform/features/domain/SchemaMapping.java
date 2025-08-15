/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.base.Splitter;
import de.ii.xtraplatform.features.domain.transform.DynamicTargetSchemaTransformer;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformation;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true, builder = "new", attributeBuilderDetection = true)
public interface SchemaMapping extends SchemaMappingBase<FeatureSchema> {

  List<DynamicTargetSchemaTransformer> getDynamicTransformers();

  @Override
  default FeatureSchema schemaWithGeometryType(FeatureSchema schema, GeometryType geometryType) {
    return new ImmutableFeatureSchema.Builder().from(schema).geometryType(geometryType).build();
  }

  @Override
  default List<FeatureSchema> getSchemasForTargetPath(List<String> path) {
    if (!getDynamicTransformers().isEmpty()) {
      List<String> transformedPath = path;
      boolean transformed = false;

      for (DynamicTargetSchemaTransformer transformer : getDynamicTransformers()) {
        if (transformer.isApplicableDynamic(transformedPath)) {
          transformedPath = transformer.transformPathDynamic(transformedPath);
          transformed = true;
        }
      }

      if (transformed) {
        List<FeatureSchema> transformedSchemas =
            SchemaMappingBase.super.getSchemasForTargetPath(transformedPath);

        for (DynamicTargetSchemaTransformer transformer : getDynamicTransformers()) {
          if (transformer.isApplicableDynamic(path)) {
            transformedSchemas = transformer.transformSchemaDynamic(transformedSchemas, path);
          }
        }

        return transformedSchemas;
      }
    }

    return SchemaMappingBase.super.getSchemasForTargetPath(path);
  }

  static SchemaMapping of(FeatureSchema schema) {
    return new ImmutableSchemaMapping.Builder().targetSchema(schema).build();
  }

  @Override
  default List<FeatureSchema> getSchemas(
      List<String> path, List<FeatureSchema> schemas, boolean useTargetPaths) {
    if (!useTargetPaths
        && schemas.stream()
            .anyMatch(
                schema -> !schema.getCoalesce().isEmpty() && schema.getPropertyMap().isEmpty())) {
      return schemas.stream()
          .map(
              schema -> {
                if (!schema.getCoalesce().isEmpty()) {
                  for (FeatureSchema coalesce : schema.getCoalesce()) {
                    if (coalesce.getSourcePath().isPresent()) {
                      List<String> sourcePath =
                          coalesce.getConstantValue().isPresent()
                              ? List.of(coalesce.getSourcePath().get())
                              : Splitter.on('/')
                                  .omitEmptyStrings()
                                  .splitToList(coalesce.getSourcePath().get());
                      if (Objects.equals(
                          sourcePath, path.subList(path.size() - sourcePath.size(), path.size()))) {
                        ImmutableFeatureSchema build =
                            new ImmutableFeatureSchema.Builder()
                                .from(schema)
                                .sourcePath(coalesce.getSourcePath())
                                .valueType(coalesce.getValueType().orElse(coalesce.getType()))
                                .sourcePaths(List.of())
                                .coalesce(List.of())
                                .build();
                        return build;
                      }
                    }
                  }
                }

                return schema;
              })
          .collect(Collectors.toList());
    }

    return schemas;
  }

  @Override
  default List<String> cleanPath(List<String> path) {
    if (path.stream().anyMatch(elem -> elem.contains("{"))) {
      return path.stream().map(this::cleanPath).collect(Collectors.toList());
    }
    /*if (path.get(path.size() - 1).contains("{")) {
      List<String> key = new ArrayList<>(path.subList(0, path.size() - 1));
      key.add(cleanPath(path.get(path.size() - 1)));
      return key;
    }*/
    return path;
  }

  // TODO: static cleanup method in PathParser
  @Override
  default String cleanPath(String path) {
    if (path.contains("{")) {
      int i = path.indexOf("{");
      if (path.startsWith("filter", i + 1) || path.startsWith("sql", i + 1)) {
        return path.substring(0, i + 2) + cleanPath(path.substring(i + 2));
      }
      return path.substring(0, path.indexOf("{"));
    }
    return path;
  }

  // TODO: still needed?
  @Override
  default Optional<String> getPathSeparator() {
    // if (useTargetPaths()) {
    return getTargetSchema().getTransformations().stream()
        .filter(transformation -> transformation.getFlatten().isPresent())
        .findFirst()
        .flatMap(PropertyTransformation::getFlatten);
    // }

    // return Optional.empty();
  }
}
