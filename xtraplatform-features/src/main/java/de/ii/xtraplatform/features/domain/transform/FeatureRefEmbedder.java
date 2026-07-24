/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.Graphs;
import com.google.common.graph.ImmutableGraph;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.TypesResolver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @langEn Feature references can also be embedded inline instead of creating a reference/link. To
 *     always embed the referenced features, the `embed` option in the feature schema of the feature
 *     reference is set to `ALWAYS`.
 *     <p>#### Configuration
 *     <p>The `sourcePath` of the feature reference property must end at the referenced feature;
 *     that is, at least the `id` property of the reference must be declared explicitly.
 *     <p>In the following example, the `abs` column is the foreign key of the referenced feature in
 *     the `abschnitteaeste` feature type:
 *     <p><code>
 * ```yaml
 * abs:
 *   sourcePath: '[abs=abs]abschnitteaeste_line'
 *   type: FEATURE_REF
 *   embed: ALWAYS
 *   label: Abschnitt/Ast
 *   description: 16-stellige Kennung des Abschnittes oder Astes
 *   refType: abschnitteaeste
 *   properties:
 *     id:
 *       type: STRING
 *       sourcePath: abs
 * ```
 * </code>
 * @langDe Feature-Referenzen können auch inline eingebettet werden, anstatt einen Verweis/Link zu
 *     erstellen. Um die referenzierten Features immer einzubetten, wird die Option `embed` im
 *     Feature-Schema der Feature-Referenz auf `ALWAYS` gesetzt.
 *     <p>#### Konfiguration
 *     <p>Der `sourcePath` der Eigenschaft muss beim referenzierten Feature enden, d.h. zumindest
 *     die Eigenschaft `id` der Referenz muss explizit angegeben werden.
 *     <p>Im folgenden Beispiel ist die Spalte `abs` der Fremdschlüssel des referenzierten Objekts
 *     in der Objektart `abschnitteaeste`:
 *     <p><code>
 * ```yaml
 * abs:
 *   sourcePath: '[abs=abs]abschnitteaeste_line'
 *   type: FEATURE_REF
 *   embed: ALWAYS
 *   label: Abschnitt/Ast
 *   description: 16-stellige Kennung des Abschnittes oder Astes
 *   refType: abschnitteaeste
 *   properties:
 *     id:
 *       type: STRING
 *       sourcePath: abs
 * ```
 * </code>
 */
public class FeatureRefEmbedder implements TypesResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureRefEmbedder.class);

  private final String providerId;
  private final WithoutRoles withoutRoles;
  private Map<String, FeatureSchema> allTypes;
  private List<List<String>> typesByRound;
  private int currentRound;

  public FeatureRefEmbedder(String providerId) {
    this.providerId = providerId;
    this.withoutRoles = new WithoutRoles();
    this.allTypes = Map.of();
    this.typesByRound = List.of();
    this.currentRound = -1;
  }

  @Override
  public int maxRounds() {
    return typesByRound.size();
  }

  @Override
  public boolean needsResolving(Map<String, FeatureSchema> types) {
    if (allTypes.isEmpty()) {
      this.allTypes = new LinkedHashMap<>(types);
      this.typesByRound = getTypesByRound(types, providerId);
    }

    return !typesByRound.isEmpty() && TypesResolver.super.needsResolving(types);
  }

  @Override
  public boolean needsResolving(
      FeatureSchema property, boolean isFeature, boolean isInConcat, boolean isInCoalesce) {
    return property.isEmbed() && property.getRole().orElse(null) != Role.EMBEDDED_FEATURE;
  }

  @Override
  public Map<String, FeatureSchema> resolve(Map<String, FeatureSchema> types) {
    this.currentRound++;

    Map<String, FeatureSchema> newTypes = new LinkedHashMap<>(types);
    Map<String, FeatureSchema> resolvedTypes =
        TypesResolver.super.resolve(getTypesForCurrentRound());

    newTypes.putAll(resolvedTypes);
    this.allTypes.putAll(resolvedTypes);

    return newTypes;
  }

  private Map<String, FeatureSchema> getTypesForCurrentRound() {
    if (currentRound < typesByRound.size()) {
      return typesByRound.get(currentRound).stream()
          .collect(ImmutableMap.toImmutableMap(t -> t, allTypes::get));
    }
    return Map.of();
  }

  @Override
  public FeatureSchema resolve(FeatureSchema property, List<FeatureSchema> parents) {
    if (!property.getConcat().isEmpty()) {
      return getBuilder(property, allTypes)
          .map(
              b -> {
                property
                    .getConcat()
                    .forEach(
                        concat -> {
                          getBuilder(concat, allTypes)
                              .ifPresent(b2 -> b.addConcat(b2.build().accept(withoutRoles)));
                        });
                return b.build();
              })
          .orElse(null);
    }

    if (!property.getCoalesce().isEmpty()) {
      return getBuilder(property, allTypes)
          .map(
              b -> {
                property
                    .getCoalesce()
                    .forEach(
                        coalesce -> {
                          getBuilder(coalesce, allTypes)
                              .ifPresent(b2 -> b.addCoalesce(b2.build().accept(withoutRoles)));
                        });
                return b.build();
              })
          .orElse(null);
    }

    return getBuilder(property, allTypes).map(b -> b.build().accept(withoutRoles)).orElse(null);
  }

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity", "PMD.NPathComplexity"})
  private static Optional<Builder> getBuilder(
      FeatureSchema schema, Map<String, FeatureSchema> types) {
    String ref =
        schema
            .getRefType()
            .or(
                () ->
                    schema.getProperties().stream()
                        .filter(p -> "type".equals(p.getName()))
                        .findFirst()
                        .flatMap(FeatureSchema::getConstantValue))
            .orElse(null);
    if (Objects.isNull(ref) && schema.getConcat().isEmpty() && schema.getCoalesce().isEmpty()) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(
            "The referenced type of a feature reference cannot be determined. Property: {}.",
            schema.getFullPathAsString());
      }
      return Optional.empty();
    }

    FeatureSchema refSchema = Objects.nonNull(ref) ? types.get(ref) : null;
    if (Objects.isNull(refSchema)
        && schema.getConcat().isEmpty()
        && schema.getCoalesce().isEmpty()) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(
            "The schema of a referenced type of a feature reference cannot be determined. Property: {}.",
            schema.getFullPathAsString());
      }
      return Optional.empty();
    }

    Optional<String> objectType =
        Objects.nonNull(refSchema) ? refSchema.getObjectType() : Optional.ofNullable(ref);

    Builder builder =
        new Builder()
            .from(schema)
            .type(schema.isArray() ? Type.OBJECT_ARRAY : Type.OBJECT)
            .objectType(objectType)
            .refType(Optional.empty())
            .embed(Optional.empty())
            .role(Optional.of(Role.EMBEDDED_FEATURE))
            .concat(List.of())
            .coalesce(List.of());

    if (Objects.nonNull(refSchema)) {
      builder.propertyMap(refSchema.getPropertyMap());
      if (schema.getLabel().isEmpty()) {
        builder.label(refSchema.getLabel().orElse(refSchema.getName()));
      }
      if (schema.getDescription().isEmpty()) {
        builder.description(refSchema.getDescription());
      }
    } else {
      builder.propertyMap(Map.of());
    }

    return Optional.of(builder);
  }

  @SuppressWarnings({"UnstableApiUsage", "PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  private static List<List<String>> getTypesByRound(
      Map<String, FeatureSchema> types, String providerId) {
    // determine graph of feature refs
    Graph<String> graph = getEmbeds(types);
    if (Graphs.hasCycle(graph)) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(
            "Feature provider with id '{}' has a cycle in the feature references that are embedded. No feature references will be embedded.",
            providerId);
      }
      return List.of();
    }

    Graph<String> graph2 = Graphs.transitiveClosure(graph);
    Map<String, Integer> map = new HashMap<>();
    int prio = 0;
    int numTypes = types.keySet().size();
    while (map.keySet().size() < numTypes && prio < numTypes) {
      int currentPrio = prio;
      Map<String, Integer> map2 = Map.copyOf(map);
      types.forEach(
          (key, value) -> {
            if (!map.containsKey(key)
                && (!graph2.nodes().contains(key)
                    || graph2.successors(key).stream()
                        .filter(n -> !n.equals(key))
                        .allMatch(map2::containsKey))) {
              map.put(key, currentPrio);
            }
          });
      prio++;
    }

    if (map.keySet().size() < numTypes) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(
            "Internal error while analyzing feature provider with id '{}' for feature references that should be embedded. No feature references will be embedded. Missed types: '{}'. Graph: {}",
            providerId,
            types.keySet().stream()
                .filter(k -> !map.containsKey(k))
                .collect(Collectors.joining(", ")),
            graph);
      }
      return List.of();
    }

    List<List<String>> typesByPriority = new ArrayList<>();

    for (int i = 1; i < prio; i++) {
      for (Map.Entry<String, Integer> entry : map.entrySet()) {
        if (entry.getValue() == i) {
          if (typesByPriority.size() <= i - 1) {
            typesByPriority.add(new ArrayList<>());
          }
          typesByPriority.get(i - 1).add(entry.getKey());
        }
      }
    }

    return typesByPriority;
  }

  @SuppressWarnings("UnstableApiUsage")
  private static Graph<String> getEmbeds(Map<String, FeatureSchema> types) {
    ImmutableGraph.Builder<String> builder =
        GraphBuilder.directed().allowsSelfLoops(false).immutable();
    types.forEach(
        (key, value) ->
            Stream.concat(
                    value.getAllNestedProperties().stream(),
                    value.getConcat().stream()
                        .flatMap(
                            (FeatureSchema featureSchema) ->
                                featureSchema.getAllNestedProperties().stream()))
                .filter(SchemaBase::isEmbed)
                .forEach(p -> addEmbeddingEdges(builder, key, p)));
    return builder.build();
  }

  private static void addEmbeddingEdges(
      ImmutableGraph.Builder<String> builder, String key, FeatureSchema property) {
    if (property.getConcat().isEmpty()) {
      if (property.getCoalesce().isEmpty()) {
        property
            .getRefType()
            .filter(ref -> shouldEmbedRef(key, property, ref))
            .ifPresent(ref -> builder.putEdge(key, ref));
        return;
      }

      property.getCoalesce().stream()
          .map(FeatureSchema::getRefType)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .filter(ref -> !ref.equals(key))
          .forEach(ref -> builder.putEdge(key, ref));
      return;
    }

    property.getConcat().stream()
        .map(FeatureSchema::getRefType)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(ref -> !ref.equals(key))
        .forEach(ref -> builder.putEdge(key, ref));
  }

  private static boolean shouldEmbedRef(String key, FeatureSchema property, String ref) {
    if (!ref.equals(key)) {
      return true;
    }
    if (LOGGER.isWarnEnabled()) {
      LOGGER.warn(
          "Feature type with id '{}' has a feature reference that embeds itself at '{}'. The feature reference will not be embedded.",
          key,
          property.getFullPathAsString());
    }
    return false;
  }
}
