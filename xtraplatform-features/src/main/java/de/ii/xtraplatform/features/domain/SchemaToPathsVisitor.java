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
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaToPathsVisitor<T extends SchemaBase<T>>
    implements SchemaVisitor<T, Multimap<List<String>, T>> {

  static final Splitter SPLITTER = Splitter.on('/').omitEmptyStrings();
  static final BiFunction<String, Boolean, String> IDENTITY = (path, isValue) -> path;

  private final BiFunction<String, Boolean, String> sourcePathTransformer;
  private final boolean useTargetPath;
  private int counter;

  SchemaToPathsVisitor(boolean useTargetPath) {
    this(useTargetPath, IDENTITY);
  }

  SchemaToPathsVisitor(
      boolean useTargetPath, BiFunction<String, Boolean, String> sourcePathTransformer) {
    this.sourcePathTransformer = sourcePathTransformer;
    this.useTargetPath = useTargetPath;
    this.counter = 0;
  }

  private static List<String> appendToLast(List<String> list, String suffix) {
    if (list.isEmpty()) {
      return list;
    }
    List<String> newList = new ArrayList<>(list.subList(0, list.size() - 1));
    newList.add(list.get(list.size() - 1) + suffix);

    return newList;
  }

  @Override
  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  public Multimap<List<String>, T> visit(
      T schema, List<Multimap<List<String>, T>> visitedProperties) {
    counter++;

    List<List<String>> paths =
        useTargetPath
            ? ImmutableList.of(appendToLast(schema.getPath(), "{priority=" + counter + "}")) // )
            // TODO: static cleanup method in PathParser
            : schema.getEffectiveSourcePaths().stream()
                .map(
                    sourcePath1 -> {
                      String sourcePath =
                          sourcePathTransformer.apply(sourcePath1, schema.isValue());

                      int i = sourcePath.indexOf('{');

                      if (i > 0) {
                        List<String> p =
                            new ArrayList<>(SPLITTER.splitToList(sourcePath.substring(0, i)));
                        p.set(
                            p.size() - 1,
                            p.get(p.size() - 1)
                                + sourcePath
                                    .substring(i)
                                    .replaceAll("\\{constant=.*?'\\}", "")
                                    .replaceAll("\\{sortKey=.*?\\}", "")
                                    .replaceAll("\\{primaryKey=.*?\\}", "")
                                + "{priority="
                                + counter
                                + "}");
                        return p.stream()
                            .flatMap(
                                s -> {
                                  if (s.replaceAll("\\{.+?=.*?\\}", "").contains("/")) {
                                    return SPLITTER.splitToList(s).stream();
                                  }
                                  return Stream.of(s);
                                })
                            .collect(Collectors.toList());
                      }

                      return SPLITTER.splitToList(sourcePath + "{priority=" + counter + "}");
                    })
                .collect(Collectors.toList());

    if (!useTargetPath && paths.isEmpty()) {
      paths = List.of(List.of("__EMPTY__", String.valueOf(counter)));
    }

    if (paths.isEmpty()) {
      return visitedProperties.stream()
          .flatMap(
              map ->
                  map.asMap().entrySet().stream().flatMap(entry -> prependToKey(entry, List.of())))
          .collect(
              ImmutableListMultimap.toImmutableListMultimap(
                  Map.Entry::getKey, Map.Entry::getValue));
    }

    if (!useTargetPath
        && (schema.getType() == Type.OBJECT_ARRAY || schema.isFeature())
        && schema instanceof FeatureSchema
        && (!((FeatureSchema) schema).getConcat().isEmpty()
            || !((FeatureSchema) schema).getCoalesce().isEmpty())) {
      return Stream.concat(
              paths.stream().map(path -> Map.entry(path, schema)),
              visitedProperties.stream()
                  .flatMap(
                      map ->
                          map.asMap().entrySet().stream()
                              .flatMap(entry -> prependToKey(entry, List.of()))))
          .collect(
              ImmutableListMultimap.toImmutableListMultimap(
                  Map.Entry::getKey, Map.Entry::getValue));
    }

    return paths.stream()
        .flatMap(
            path ->
                Stream.concat(
                    Stream.of(Map.entry(path, schema))
                        .map(
                            entry -> {
                              // special handling for virtual objects (no source path) in concat
                              // the virtual object has to be ignored since it cannot be
                              // differentiated from its parent object
                              // it will be re-added automatically by the wrap transformer
                              if (!useTargetPath
                                  && entry.getValue().isVirtualObject()
                                  && MappingOperationResolver.isConcatPath(
                                      entry.getValue().getPath())) {
                                return Map.entry(
                                    List.of("__EMPTY__", String.valueOf(counter)),
                                    entry.getValue());
                              }

                              return entry;
                            }),
                    visitedProperties.stream()
                        .flatMap(
                            map ->
                                map.asMap().entrySet().stream()
                                    .flatMap(entry -> prependToKey(entry, path)))))
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
    if (parentPath.isEmpty() || Objects.equals(parentPath.get(0), "__EMPTY__")) {
      return path;
    }

    return ImmutableList.<String>builder().addAll(parentPath).addAll(path).build();
  }
}
