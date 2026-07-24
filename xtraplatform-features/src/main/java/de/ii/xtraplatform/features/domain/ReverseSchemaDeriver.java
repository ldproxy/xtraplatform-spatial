/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface ReverseSchemaDeriver<T extends SchemaBase<T>>
    extends SchemaVisitor<FeatureSchema, List<T>> {

  Splitter SPLITTER = Splitter.on('/').omitEmptyStrings();

  @Override
  default List<T> visit(FeatureSchema schema, List<List<T>> visitedProperties) {

    if (schema.isConstant()) {
      return ImmutableList.of();
    }

    List<String> currentPath = splitPath(schema.getSourcePath().orElse(ignore()));

    Map<List<String>, T> objectCache = new LinkedHashMap<>();

    List<T> properties =
        visitedProperties.stream()
            .flatMap(List::stream)
            .map(
                sourceSchema -> {
                  List<String> parentPath = sourceSchema.getParentPath();

                  if (parentPath.isEmpty()) {
                    return sourceSchema;
                  }

                  String parentParentPath = currentPath.get(currentPath.size() - 1);

                  List<T> parents = createParents(parentParentPath, sourceSchema, objectCache);

                  if (!parents.isEmpty()) {
                    parents.forEach(t -> objectCache.put(t.getPath(), t));

                    return parents.get(0);
                  }

                  return sourceSchema;
                })
            .collect(Collectors.toList());

    properties =
        properties.stream()
            .map(sourceSchema -> objectCache.getOrDefault(sourceSchema.getPath(), sourceSchema))
            .distinct()
            .map(sourceSchema -> prependToParentPath(currentPath, sourceSchema))
            .collect(Collectors.toList());

    if (shouldIgnore(currentPath)) {
      return properties.stream()
          .map(sourceSchema -> prependToSourcePath(schema.getName(), sourceSchema))
          .collect(Collectors.toList());
    }

    T current = create(currentPath, schema);

    return ImmutableList.of(addChildren(current, properties));
  }

  List<T> createParents(String parentParentPath, T child, Map<List<String>, T> objectCache);

  T create(List<String> path, FeatureSchema targetSchema);

  T create(String path, FeatureSchema targetSchema);

  T addChildren(T parent, List<T> children);

  T prependToParentPath(List<String> path, T schema);

  T prependToSourcePath(String parentSourcePath, T schema);

  default List<String> splitPath(String path) {
    return SPLITTER.splitToList(path);
  }

  String ignore();

  boolean shouldIgnore(List<String> path);
}
