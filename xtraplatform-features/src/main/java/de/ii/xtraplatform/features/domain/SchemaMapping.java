/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.ImmutableMappingInfo.Builder;
import de.ii.xtraplatform.features.domain.transform.DynamicTargetSchemaTransformer;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformation;
import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true, builder = "new", attributeBuilderDetection = true)
public interface SchemaMapping extends SchemaMappingBase<FeatureSchema> {

  Optional<DynamicTargetSchemaTransformer> getDynamicTransformer();

  @Override
  default FeatureSchema schemaWithGeometryType(
      FeatureSchema schema, SimpleFeatureGeometry geometryType) {
    return new ImmutableFeatureSchema.Builder().from(schema).geometryType(geometryType).build();
  }

  @Override
  default List<FeatureSchema> getSchemasForTargetPath(List<String> path) {
    if (getDynamicTransformer().isPresent()) {
      DynamicTargetSchemaTransformer transformer = getDynamicTransformer().get();
      if (transformer.isApplicableDynamic(path)) {
        List<FeatureSchema> schemas =
            getSchemasByTargetPath()
                .getOrDefault(transformer.transformPathDynamic(path), ImmutableList.of());

        return transformer.transformSchemaDynamic(schemas, path);
      }
    }

    return SchemaMappingBase.super.getSchemasForTargetPath(path);
  }

  static SchemaMapping of(FeatureSchema schema) {
    return new ImmutableSchemaMapping.Builder().targetSchema(schema).build();
  }

  @Value.Derived
  @Value.Auxiliary
  default Map<List<String>, List<MappingInfo>> forSourcePath() {
    return forPath(
        new SchemaToPathsVisitor<>(false, getSourcePathTransformer()), this::cleanPath, false);
  }

  @Value.Derived
  @Value.Auxiliary
  default Map<List<String>, List<MappingInfo>> forTargetPath() {
    return forPath(new SchemaToPathsVisitor<>(true), this::cleanPath, true);
  }

  // TODO: needed?
  default Map<List<String>, List<MappingInfo>> forPath(
      SchemaToPathsVisitor<FeatureSchema> pathsVisitor,
      Function<List<String>, List<String>> pathCleaner,
      boolean useTargetPath) {
    return getTargetSchema().accept(pathsVisitor).asMap().keySet().stream()
        .map(pathCleaner)
        .map(
            path -> {
              List<FeatureSchema> schemas =
                  useTargetPath ? getSchemasForTargetPath(path) : getSchemasForSourcePath(path);
              List<Integer> positions =
                  useTargetPath ? getPositionsForTargetPath(path) : getPositionsForSourcePath(path);
              List<List<FeatureSchema>> parentSchemas =
                  useTargetPath
                      ? getParentSchemasForTargetPath(path)
                      : getParentSchemasForSourcePath(path);
              List<List<Integer>> parentPositions =
                  useTargetPath
                      ? getParentPositionsForTargetPath(path)
                      : getParentPositionsForSourcePath(path);

              boolean b =
                  schemas.size() == positions.size()
                      && schemas.size() == parentSchemas.size()
                      && schemas.size() == parentPositions.size()
                      && parentPositions.stream().flatMap(List::stream).noneMatch(pos -> pos == -1);

              Preconditions.checkState(b);

              // TODO: if there is ever more than one value, loses position of duplicates
              List<MappingInfo> mappingInfos = new ArrayList<>();
              for (int i = 0; i < schemas.size(); i++) {
                mappingInfos.add(
                    new Builder()
                        .schema(schemas.get(i))
                        .position(positions.get(i))
                        .parentSchemas(parentSchemas.get(i))
                        .parentPositions(parentPositions.get(i))
                        .build());
              }

              return new SimpleImmutableEntry<>(path, mappingInfos);
            })
        .collect(
            ImmutableMap.toImmutableMap(
                Entry::getKey,
                Entry::getValue,
                (first, second) -> {
                  ArrayList<MappingInfo> mappingInfos = new ArrayList<>(first);
                  mappingInfos.addAll(second);
                  return mappingInfos;
                }));
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
      if (path.startsWith("filter", i + 1)) {
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
