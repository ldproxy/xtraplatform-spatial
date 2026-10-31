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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaToMappingVisitor<T extends SchemaBase<T>>
    implements SchemaVisitor<T, Multimap<List<String>, T>> {

  static final Splitter SPLITTER = Splitter.on('/').omitEmptyStrings();

  private final BiFunction<String, Boolean, String> sourcePathTransformer;
  private final boolean useTargetPath;

  public SchemaToMappingVisitor(BiFunction<String, Boolean, String> sourcePathTransformer) {
    this(false, sourcePathTransformer);
  }

  public SchemaToMappingVisitor(
      boolean useTargetPath, BiFunction<String, Boolean, String> sourcePathTransformer) {
    this.sourcePathTransformer = sourcePathTransformer;
    this.useTargetPath = useTargetPath;
  }

  @Override
  public Multimap<List<String>, T> visit(
      T schema, List<Multimap<List<String>, T>> visitedProperties) {
    List<String> path2 =
        useTargetPath
            ? schema.getPath()
            // NOTE: static cleanup method in PathParser
            : SPLITTER.splitToList(
                schema
                    .getSourcePath()
                    .map(sourcePath -> sourcePath.replaceAll("\\{constant=.*?\\}", ""))
                    .orElse(""));

    List<String> path =
        path2.stream()
            .map(path1 -> sourcePathTransformer.apply(path1, schema.isValue()))
            .collect(Collectors.toList());

    return Stream.concat(
            path.isEmpty()
                ? Stream.empty()
                : Stream.of(new AbstractMap.SimpleImmutableEntry<>(path, schema)),
            visitedProperties.stream()
                .flatMap(
                    map ->
                        map.asMap().entrySet().stream()
                            .flatMap(entry -> prependToKey(entry, path))))
        .collect(
            ImmutableListMultimap.toImmutableListMultimap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Stream<Map.Entry<List<String>, T>> prependToKey(
      Map.Entry<List<String>, Collection<T>> property, List<String> parentPath) {
    return property.getValue().stream()
        .map(
            value ->
                new AbstractMap.SimpleImmutableEntry<>(
                    merge(parentPath, property.getKey()), value));
  }

  private List<String> merge(List<String> parentPath, List<String> path) {
    if (parentPath.isEmpty()) {
      return path;
    }

    return ImmutableList.<String>builder().addAll(parentPath).addAll(path).build();
  }
}
