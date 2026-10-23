/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder;
import de.ii.xtraplatform.features.domain.ImmutablePartialObjectSchema;
import de.ii.xtraplatform.features.domain.PartialObjectSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaFragmentResolver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class LocalSchemaFragmentResolver implements SchemaFragmentResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalSchemaFragmentResolver.class);

  @Inject
  public LocalSchemaFragmentResolver() {}

  private String getKey(String ref) {
    return ref.replace("#/fragments/", "");
  }

  @Override
  public boolean canResolve(String ref, FeatureProviderDataV2 data) {
    return Objects.nonNull(ref) && ref.startsWith("#/fragments/");
  }

  @Override
  public FeatureSchema resolve(String ref, FeatureSchema original, FeatureProviderDataV2 data) {
    FeatureSchema resolved = resolve(ref, data);

    return merge(original, resolved);
  }

  @Override
  public PartialObjectSchema resolve(
      String ref, PartialObjectSchema original, FeatureProviderDataV2 data) {
    FeatureSchema resolved = resolve(ref, data);

    return merge(original, resolved);
  }

  private FeatureSchema merge(FeatureSchema original, FeatureSchema resolved) {
    if (original.getIgnore()) {
      return null;
    }

    String resolvedOrigin = resolved.getObjectType().orElse(null);
    Map<String, FeatureSchema> properties =
        merge(original.getPropertyMap(), resolved.getPropertyMap(), resolvedOrigin);

    Builder builder =
        new Builder()
            .from(resolved)
            .from(original)
            .type(original.getType() == Type.STRING ? resolved.getType() : original.getType())
            .schema(resolved.getSchema())
            .propertyMap(properties);

    if (!resolved.getMerge().isEmpty() && !original.getPropertyMap().isEmpty()) {
      PartialObjectSchema last = resolved.getMerge().get(resolved.getMerge().size() - 1);
      last = merge(original, last);

      builder.merge(resolved.getMerge().subList(0, resolved.getMerge().size() - 1));
      builder.addMerge(last);
      builder.propertyMap(Map.of());
    }

    return builder.build();
  }

  private PartialObjectSchema merge(PartialObjectSchema original, FeatureSchema resolved) {
    String resolvedOrigin = resolved.getObjectType().orElse(null);
    Map<String, FeatureSchema> properties =
        merge(original.getPropertyMap(), resolved.getPropertyMap(), resolvedOrigin);

    return new ImmutablePartialObjectSchema.Builder()
        .from(original)
        .sourcePath(resolved.getSourcePath())
        .schema(resolved.getSchema())
        .propertyMap(properties)
        .build();
  }

  private PartialObjectSchema merge(FeatureSchema original, PartialObjectSchema resolved) {
    // PartialObjectSchema doesn't carry an objectType — its properties keep whatever origin tag
    // they already had (set when their own fragment was resolved earlier in the chain).
    Map<String, FeatureSchema> properties =
        merge(original.getPropertyMap(), resolved.getPropertyMap(), null);

    return new ImmutablePartialObjectSchema.Builder()
        .from(resolved)
        .propertyMap(properties)
        .build();
  }

  private Map<String, FeatureSchema> merge(
      Map<String, FeatureSchema> original,
      Map<String, FeatureSchema> resolved,
      String resolvedOrigin) {
    Map<String, FeatureSchema> properties = new LinkedHashMap<>();

    resolved.forEach(
        (key, property) -> {
          FeatureSchema tagged = tagOrigin(property, resolvedOrigin);
          if (original.containsKey(key)) {
            FeatureSchema merged = merge(original.get(key), tagged);

            if (Objects.nonNull(merged)) {
              properties.put(key, merged);
            }
          } else {
            properties.put(key, tagged);
          }
        });
    original.forEach(
        (key, property) -> {
          if (!resolved.containsKey(key)) {
            properties.put(key, property);
          }
        });

    return properties;
  }

  // Tags `property` (and recursively its sub-properties) with `originObjectType = originType` —
  // but only where the tag is missing. An existing tag wins so an outer fragment's listing
  // doesn't get overwritten by a base fragment further down the chain. Codecs that qualify
  // property element names per object type (notably GML with `objectTypeNamespaces`) read this
  // tag at runtime; without it they'd inherit the containing feature's objectType, which is
  // wrong for properties that come from a different schema fragment than the feature itself.
  private static FeatureSchema tagOrigin(FeatureSchema property, String originType) {
    if (originType == null) {
      return property;
    }
    boolean missing = property.getOriginObjectType().isEmpty();
    Map<String, FeatureSchema> taggedChildren = null;
    for (Map.Entry<String, FeatureSchema> e : property.getPropertyMap().entrySet()) {
      FeatureSchema childTagged = tagOrigin(e.getValue(), originType);
      if (childTagged != e.getValue()) {
        if (taggedChildren == null) {
          taggedChildren = new LinkedHashMap<>(property.getPropertyMap());
        }
        taggedChildren.put(e.getKey(), childTagged);
      }
    }
    if (!missing && taggedChildren == null) {
      return property;
    }
    Builder b = new Builder().from(property);
    if (missing) {
      b.originObjectType(originType);
    }
    if (taggedChildren != null) {
      b.propertyMap(taggedChildren);
    }
    return b.build();
  }

  private FeatureSchema resolve(String ref, FeatureProviderDataV2 data) {
    String key = getKey(ref);

    if (!data.getFragments().containsKey(key)) {
      throw new IllegalArgumentException("Local fragment not found: " + ref);
    }

    return data.getFragments().get(key);
  }
}
