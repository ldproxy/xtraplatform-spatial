/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import static de.ii.xtraplatform.features.domain.MappingRule.ROOT_TARGET;

import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.domain.MappingRule.Scope;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class MappingRulesDeriver
    implements SchemaVisitorWithFinalizer<FeatureSchema, List<MappingRule>, List<MappingRule>> {

  private static final String VALUE_ARRAY_VALUE_SUFFIX = ".[]";

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
                        ? Stream.empty()
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

    return Stream.concat(rules, visitedProperties.stream().flatMap(List::stream)).toList();
  }

  @Override
  public List<MappingRule> finalize(FeatureSchema featureSchema, List<MappingRule> mappingRules) {
    Map<String, List<MappingRule>> rulesByTable = new LinkedHashMap<>();
    Map<String, Integer> schemaIndexes = new HashMap<>();

    for (MappingRule rule : mappingRules) {
      // tables
      if (rule.isObjectOrArray()) {
        MappingRule cleanRule = cleanup(rule);
        // distinct
        if (rulesByTable.containsKey(rule.getIdentifier())
            && rulesByTable.get(rule.getIdentifier()).contains(cleanRule)) {
          continue;
        }

        rulesByTable.put(rule.getIdentifier(), new ArrayList<>(List.of(cleanRule)));

        continue;
      }

      if (!schemaIndexes.containsKey(rule.getSource())) {
        schemaIndexes.put(rule.getSource(), 0);
      } else {
        schemaIndexes.put(rule.getSource(), schemaIndexes.get(rule.getSource()) + 1);
      }

      if (schemaIndexes.get(rule.getSource()) > 0) {
        rule =
            new ImmutableMappingRule.Builder()
                .from(rule)
                .index(schemaIndexes.get(rule.getSource()))
                .build();
      }

      String tableIdentifier = rule.getIdentifierParent();

      // implicit tables
      if (rule.hasSourceParent() && !rulesByTable.containsKey(tableIdentifier)) {
        MappingRule finalRule = rule;
        List<String> matchingTables =
            rulesByTable.keySet().stream()
                .filter(id -> id.startsWith(finalRule.getSourceParent() + "-"))
                .toList();

        if (matchingTables.isEmpty()) {
          MappingRule tableRule =
              new ImmutableMappingRule.Builder()
                  .source(rule.getSourceParent())
                  .target(
                      rule.getTarget().contains(".")
                          ? rule.getTarget().substring(0, rule.getTarget().lastIndexOf("."))
                          : ROOT_TARGET)
                  .type(
                      rule.getTarget().endsWith(VALUE_ARRAY_VALUE_SUFFIX)
                          ? Type.VALUE_ARRAY
                          : Type.OBJECT_ARRAY)
                  .index(0)
                  .build();

          rulesByTable.put(tableIdentifier, new ArrayList<>(List.of(tableRule, cleanup(rule))));

          continue;
        }

        // add to last matching table
        tableIdentifier = matchingTables.get(matchingTables.size() - 1);
      }

      rulesByTable.get(tableIdentifier).add(cleanup(rule));
    }

    return sorted(rulesByTable);
  }

  private static MappingRule cleanup(MappingRule rule) {
    if (rule.getTarget().endsWith(VALUE_ARRAY_VALUE_SUFFIX)) {
      return new ImmutableMappingRule.Builder()
          .from(rule)
          .target(
              rule.getTarget().substring(0, rule.getTarget().lastIndexOf(VALUE_ARRAY_VALUE_SUFFIX)))
          .build();
    }
    return rule;
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

            for (int j = span; j < cursors.size(); j++) {
              cursors.set(j, cursors.get(j) + rulesByTable.get(tableIdentifier).size());
            }

            break;
          }
          if (source.startsWith(parents.get(i) + "/")) {
            int span = spans.get(i);
            int index = cursors.get(span);
            sorted.addAll(index, rulesByTable.get(tableIdentifier));

            cursors.add(span + 1, index + rulesByTable.get(tableIdentifier).size());
            parents.add(span + 1, source);
            spans.add(span + 1, span + 1);

            spans.set(i, span + 1);
            for (int j = 0; j < i; j++) {
              if (spans.get(j) == span) {
                spans.set(j, span + 1);
              }
            }
            for (int j = span + 2; j < spans.size(); j++) {
              spans.set(j, spans.get(j) + 1);
            }

            cursors.set(i, index + rulesByTable.get(tableIdentifier).size());
            for (int j = 0; j < i; j++) {
              if (cursors.get(j) == index) {
                cursors.set(j, index + rulesByTable.get(tableIdentifier).size());
              }
            }
            for (int j = span + 2; j < cursors.size(); j++) {
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
    String target =
        schema.isFeature()
            ? ROOT_TARGET
            : schema.getType() == Type.VALUE_ARRAY
                ? schema.getFullPathAsString() + VALUE_ARRAY_VALUE_SUFFIX
                : schema.getFullPathAsString();
    Type type =
        schema.isFeature()
            ? Type.OBJECT_ARRAY
            : schema.getType() == Type.VALUE_ARRAY
                ? schema.getValueType().orElse(Type.STRING)
                : schema.getType();

    ImmutableMappingRule.Builder rule =
        new ImmutableMappingRule.Builder()
            .source(parentSourcePath + sourcePath)
            .target(target)
            .type(type)
            .role(schema.getRole())
            .scope(toScope(schema.getExcludedScopes()))
            .index(0);

    return Stream.of(rule.build());
  }

  private static Optional<Scope> toScope(Set<SchemaBase.Scope> ex) {
    if (ex.isEmpty()) {
      return Optional.empty();
    }

    if (ex.contains(SchemaBase.Scope.RECEIVABLE)) {
      if (ex.contains(SchemaBase.Scope.RETURNABLE)) {
        return Optional.of(Scope.C);
      }
      if (ex.contains(SchemaBase.Scope.QUERYABLE) && ex.contains(SchemaBase.Scope.SORTABLE)) {
        return Optional.of(Scope.R);
      }
      return Optional.of(Scope.RC);
    }

    if (ex.contains(SchemaBase.Scope.QUERYABLE)
        && ex.contains(SchemaBase.Scope.SORTABLE)
        && !ex.contains(SchemaBase.Scope.RETURNABLE)) {
      return Optional.of(Scope.RW);
    }

    return Optional.of(Scope.W);
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
