/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import dagger.Lazy;
import de.ii.xtraplatform.features.app.LocalSchemaFragmentResolver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class SchemaReferenceResolver implements TypesResolver {

  private final FeatureProviderDataV2 data;
  private final Lazy<Set<SchemaFragmentResolver>> schemaResolvers;
  private final SchemaFragmentResolver localFragmentResolver;

  public SchemaReferenceResolver(
      FeatureProviderDataV2 data, Lazy<Set<SchemaFragmentResolver>> schemaResolvers) {
    this.data = data;
    this.schemaResolvers = schemaResolvers;
    this.localFragmentResolver = new LocalSchemaFragmentResolver();
  }

  @Override
  public int maxRounds() {
    return 16;
  }

  @Override
  public Optional<String> maxRoundsWarning() {
    return Optional.of(
        String.format(
            "Exceeded the maximum length of %d for provider schema reference chains.",
            maxRounds()));
  }

  private static boolean hasSchema(FeatureSchema type) {
    return type.getSchema().isPresent();
  }

  private static boolean hasSchema(PartialObjectSchema partial) {
    return partial.getSchema().isPresent();
  }

  private static boolean hasMergeWithSchema(FeatureSchema type) {
    return type.getMerge().stream().anyMatch(SchemaReferenceResolver::hasSchema);
  }

  private static boolean hasConcatWithSchema(FeatureSchema type) {
    return type.getConcat().stream().anyMatch(SchemaReferenceResolver::hasSchema);
  }

  private static boolean hasCoalesceWithSchema(FeatureSchema type) {
    return type.getCoalesce().stream().anyMatch(SchemaReferenceResolver::hasSchema);
  }

  private SchemaFragmentResolver getResolver(String ref) {
    // NOTE: workaround, AutoBinds currently cannot be collected and registered from the same module
    return Stream.concat(Stream.of(localFragmentResolver), schemaResolvers.get().stream())
        .filter(resolver -> resolver.canResolve(ref, data))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("No resolver found for 'schema: " + ref + "'"));
  }

  private FeatureSchema resolve(String ref, FeatureSchema original) {
    SchemaFragmentResolver resolver = getResolver(ref);

    return resolver.resolve(ref, original, data);
  }

  private PartialObjectSchema resolve(String ref, PartialObjectSchema original) {
    SchemaFragmentResolver resolver = getResolver(ref);

    return resolver.resolve(ref, original, data);
  }

  @Override
  public boolean needsResolving(
      FeatureSchema property, boolean isFeature, boolean isInConcat, boolean isInCoalesce) {
    return hasSchema(property)
        || hasMergeWithSchema(property)
        || hasConcatWithSchema(property)
        || hasCoalesceWithSchema(property);
  }

  @Override
  public boolean needsResolving(PartialObjectSchema partial) {
    return hasSchema(partial);
  }

  @Override
  public FeatureSchema resolve(FeatureSchema property, List<FeatureSchema> parents) {
    if (hasSchema(property)) {
      return resolve(property.getSchema().get(), property);
    }

    if (hasMergeWithSchema(property)) {
      List<PartialObjectSchema> partials = resolveMergePartials(property.getMerge());

      return new ImmutableFeatureSchema.Builder().from(property).merge(partials).build();
    }

    if (hasConcatWithSchema(property)) {
      List<FeatureSchema> partials = resolvePartials(property.getConcat());

      return new ImmutableFeatureSchema.Builder()
          .from(property)
          .concat(partials)
          .propertyMap(Map.of())
          .build();
    }

    if (hasCoalesceWithSchema(property)) {
      List<FeatureSchema> partials = resolvePartials(property.getCoalesce());

      return new ImmutableFeatureSchema.Builder()
          .from(property)
          .coalesce(partials)
          .propertyMap(Map.of())
          .build();
    }

    return property;
  }

  private List<PartialObjectSchema> resolveMergePartials(List<PartialObjectSchema> partials) {
    List<PartialObjectSchema> resolved = new ArrayList<>();

    for (PartialObjectSchema partial : partials) {
      if (hasSchema(partial)) {
        PartialObjectSchema resolvedPartial = resolve(partial.getSchema().get(), partial);

        if (Objects.nonNull(resolvedPartial)) {
          resolved.add(resolvedPartial);
        }
      } else {
        resolved.add(partial);
      }
    }
    return resolved;
  }

  private List<FeatureSchema> resolvePartials(List<FeatureSchema> partials) {
    List<FeatureSchema> resolved = new ArrayList<>();

    for (FeatureSchema partial : partials) {
      if (hasSchema(partial)) {
        FeatureSchema resolvedPartial = resolve(partial.getSchema().get(), partial);

        if (Objects.nonNull(resolvedPartial)) {
          resolved.add(resolvedPartial);
        }
      } else {
        resolved.add(partial);
      }
    }
    return resolved;
  }
}
