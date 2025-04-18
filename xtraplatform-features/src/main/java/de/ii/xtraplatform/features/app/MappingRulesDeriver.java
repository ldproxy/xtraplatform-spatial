/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.app;

import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableMappingRule;
import de.ii.xtraplatform.features.domain.MappingRule;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaVisitorWithFinalizer;
import java.util.ArrayList;
import java.util.Collection;
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
                        ? Stream.of(Tuple.of(parentPath.substring(0, parentPath.length() - 1), ""))
                        : schema.getEffectiveSourcePaths().stream()
                            .map(childPath -> Tuple.of(parentPath, childPath)));

    Stream<MappingRule> rules =
        allPaths.flatMap(path -> toRules(path.first(), path.second(), schema));

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

      rulesByTable.get(tableIdentifier).add(rule);
    }

    return rulesByTable.values().stream().flatMap(Collection::stream).toList();
  }

  private Stream<MappingRule> toRules(
      String parentSourcePath, String sourcePath, FeatureSchema schema) {
    ImmutableMappingRule.Builder rule =
        new ImmutableMappingRule.Builder()
            .source(parentSourcePath + sourcePath)
            .target(schema.isFeature() ? "$" : schema.getFullPathAsString())
            .type(schema.isFeature() ? Type.OBJECT_ARRAY : schema.getType())
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
              .index(0)
              .build();

      rule.target(rule.build().getTarget() + ".[]").type(schema.getValueType().orElse(Type.STRING));

      return Stream.concat(Stream.of(tableRule), Stream.of(rule.build()));
    }

    return Stream.of(rule.build());
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
