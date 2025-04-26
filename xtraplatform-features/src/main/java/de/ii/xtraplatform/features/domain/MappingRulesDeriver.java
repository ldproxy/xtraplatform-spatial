/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class MappingRulesDeriver
    implements SchemaVisitorWithFinalizer<FeatureSchema, List<MappingRule>, List<MappingRule>> {

  @Override
  public List<MappingRule> visit(
      FeatureSchema schema,
      List<FeatureSchema> parents,
      List<List<MappingRule>> visitedProperties) {
    Stream<Tuple<String, String>> allPaths =
        getParentPaths(parents, "")
            .flatMap(
                parentPath ->
                    schema.getEffectiveSourcePaths().isEmpty()
                        ? Stream
                            .empty() // .of(Tuple.of(parentPath.substring(0, parentPath.length() -
                        // 1), ""))
                        : schema.getEffectiveSourcePaths().stream()
                            .map(childPath -> Tuple.of(parentPath, childPath)));

    Stream<MappingRule> rules =
        allPaths.flatMap(
            path -> {
              if (!parents.isEmpty()
                  && !parents.get(parents.size() - 1).getConcat().isEmpty()
                  && parents
                      .get(parents.size() - 1)
                      .getEffectiveSourcePaths()
                      .contains(path.second())) {
                return Stream.empty();
              }

              return toRules(path.first(), path.second(), schema);
            });

    // TODO: remove
    List<String> parentPathsList = getParentPaths(parents, "").toList();
    List<Tuple<String, String>> allPathsList =
        getParentPaths(parents, "")
            .flatMap(
                parentPath ->
                    schema.getEffectiveSourcePaths().isEmpty()
                        ? Stream
                            .empty() // .of(Tuple.of(parentPath.substring(0, parentPath.length() -
                        // 1), ""))
                        : schema.getEffectiveSourcePaths().stream()
                            .map(childPath -> Tuple.of(parentPath, childPath)))
            .toList();
    List<MappingRule> rulesList =
        allPathsList.stream()
            .flatMap(path -> toRules(path.first(), path.second(), schema))
            .toList();

    return Stream.concat(rules, visitedProperties.stream().flatMap(List::stream)).toList();
  }

  @Override
  public List<MappingRule> finalize(FeatureSchema featureSchema, List<MappingRule> mappingRules) {
    Map<String, List<MappingRule>> rulesByTable = new LinkedHashMap<>();

    for (MappingRule rule : mappingRules) {
      // tables
      if (rule.isObjectOrArray()) {
        // distinct
        if (rulesByTable.containsKey(rule.getIdentifier())
            && rulesByTable.get(rule.getIdentifier()).contains(rule)) {
          continue;
        }

        rulesByTable.put(rule.getIdentifier(), new ArrayList<>(List.of(rule)));

        continue;
      }

      String tableIdentifier = rule.getIdentifierParent();

      // implicit tables
      if (rule.hasSourceParent() && !rulesByTable.containsKey(tableIdentifier)) {
        List<String> matchingTables =
            rulesByTable.keySet().stream()
                .filter(id -> id.startsWith(rule.getSourceParent() + "-"))
                .toList();

        if (matchingTables.isEmpty()) {
          MappingRule tableRule =
              new ImmutableMappingRule.Builder()
                  .source(rule.getSourceParent())
                  .target(
                      rule.getTarget().contains(".")
                          ? rule.getTarget().substring(0, rule.getTarget().lastIndexOf("."))
                          : "$")
                  .type(Type.OBJECT_ARRAY) // TODO: get from rule with same target if any
                  .index(0)
                  .build();

          rulesByTable.put(tableIdentifier, new ArrayList<>(List.of(tableRule, rule)));

          continue;
        }

        // add to last matching table
        tableIdentifier = matchingTables.get(matchingTables.size() - 1);
      }

      rulesByTable.get(tableIdentifier).add(rule);
    }

    return sorted(rulesByTable);
  }

  private static List<MappingRule> sorted(Map<String, List<MappingRule>> rulesByTable) {
    List<MappingRule> sorted = new ArrayList<>();
    List<String> parents = new ArrayList<>();
    List<Integer> spans = new ArrayList<>();
    List<Integer> cursors = new ArrayList<>();

    for (String tableIdentifier : rulesByTable.keySet()) {
      String source = tableIdentifier.substring(0, tableIdentifier.lastIndexOf("-"));
      boolean hasParent = MappingRule.maskPathAttributes(source).indexOf("/", 1) > 1;

      if (hasParent) {
        for (int i = parents.size() - 1; i >= 0; i--) {
          if (source.equals(parents.get(i))) {
            int span = spans.get(i);
            int index = cursors.get(span);
            sorted.addAll(index, rulesByTable.get(tableIdentifier));

            for (int j = i; j < cursors.size(); j++) {
              cursors.set(j, cursors.get(j) + rulesByTable.get(tableIdentifier).size());
            }

            break;
          }
          if (source.startsWith(parents.get(i))) {
            int span = spans.get(i);
            int index = cursors.get(span);
            sorted.addAll(index, rulesByTable.get(tableIdentifier));
            cursors.add(span + 1, index);
            parents.add(span + 1, source);
            spans.set(i, span + 1);
            spans.add(span + 1, span + 1);
            for (int j = span + 2; j < spans.size(); j++) {
              spans.set(j, spans.get(j) + 1);
            }

            cursors.set(i, cursors.get(i) + rulesByTable.get(tableIdentifier).size());
            for (int j = span + 1; j < cursors.size(); j++) {
              cursors.set(j, cursors.get(j) + rulesByTable.get(tableIdentifier).size());
            }

            break;
          }
        }
      } else {
        sorted.addAll(rulesByTable.get(tableIdentifier));
        cursors.add(sorted.size());
        parents.add(source);
        spans.add(parents.size() - 1);
      }
    }

    return sorted;
  }

  private Stream<MappingRule> toRules(
      String parentSourcePath, String sourcePath, FeatureSchema schema) {
    ImmutableMappingRule.Builder rule =
        new ImmutableMappingRule.Builder()
            .source(parentSourcePath + sourcePath)
            .target(schema.isFeature() ? "$" : schema.getFullPathAsString())
            .type(schema.isFeature() ? Type.OBJECT_ARRAY : schema.getType())
            .role(schema.getRole())
            // TODO
            .index(0);

    // special case because we need valueType, otherwise it would be handled in finalize
    if (schema.getType() == Type.VALUE_ARRAY) {
      String tableSourcePath =
          sourcePath.contains("/")
              ? parentSourcePath + sourcePath.substring(0, sourcePath.lastIndexOf("/"))
              : parentSourcePath.substring(0, parentSourcePath.lastIndexOf("/"));

      MappingRule tableRule =
          new ImmutableMappingRule.Builder()
              .source(tableSourcePath)
              .target(schema.getFullPathAsString())
              .type(schema.getType())
              .role(schema.getRole())
              // TODO
              .index(0)
              .build();

      rule.target(rule.build().getTarget() + VALUE_ARRAY_VALUE_SUFFIX)
          .type(schema.getValueType().orElse(Type.STRING));

      return Stream.concat(Stream.of(tableRule), Stream.of(rule.build()));
    }

    return Stream.of(rule.build());
  }

  private static final String VALUE_ARRAY_VALUE_SUFFIX = ".[]";

  public static boolean doIgnore(String path) {
    return path.endsWith(VALUE_ARRAY_VALUE_SUFFIX);
  }

  private Stream<String> getParentPaths(List<FeatureSchema> parents, String prefix) {
    if (parents.isEmpty()) {
      return Stream.of(prefix);
    }

    if (!parents.get(0).getConcat().isEmpty() || !parents.get(0).getCoalesce().isEmpty()) {
      return getParentPaths(parents.subList(1, parents.size()), prefix);
    }

    if (parents.get(0).getEffectiveSourcePaths().isEmpty()) {
      return getParentPaths(parents.subList(1, parents.size()), prefix);
    }

    return parents.get(0).getEffectiveSourcePaths().stream()
        .flatMap(
            parentPath ->
                getParentPaths(parents.subList(1, parents.size()), prefix + parentPath + "/"));
  }
}
