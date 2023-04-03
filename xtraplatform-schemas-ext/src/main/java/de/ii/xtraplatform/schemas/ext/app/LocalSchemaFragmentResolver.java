/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.schemas.ext.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutablePartialObjectSchema;
import de.ii.xtraplatform.features.domain.PartialObjectSchema;
import de.ii.xtraplatform.features.domain.SchemaFragmentResolver;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class LocalSchemaFragmentResolver implements SchemaFragmentResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalSchemaFragmentResolver.class);

  @Inject
  LocalSchemaFragmentResolver() {}

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

    Map<String, FeatureSchema> properties =
        merge(original.getPropertyMap(), resolved.getPropertyMap());

    return new ImmutableFeatureSchema.Builder()
        .from(original)
        .from(resolved)
        .schema(Optional.empty())
        .propertyMap(properties)
        .build();
  }

  private PartialObjectSchema merge(PartialObjectSchema original, FeatureSchema resolved) {
    Map<String, FeatureSchema> properties =
        merge(original.getPropertyMap(), resolved.getPropertyMap());

    return new ImmutablePartialObjectSchema.Builder()
        .from(original)
        .sourcePath(resolved.getSourcePath())
        .schema(Optional.empty())
        .propertyMap(properties)
        .build();
  }

  private Map<String, FeatureSchema> merge(
      Map<String, FeatureSchema> original, Map<String, FeatureSchema> resolved) {
    Map<String, FeatureSchema> properties = new LinkedHashMap<>();

    resolved.forEach(
        (key, property) -> {
          if (original.containsKey(key)) {
            FeatureSchema merged = merge(original.get(key), property);

            if (Objects.nonNull(merged)) {
              properties.put(key, merged);
            }
          } else {
            properties.put(key, property);
          }
        });

    return properties;
  }

  private FeatureSchema resolve(String ref, FeatureProviderDataV2 data) {
    String key = getKey(ref);

    if (!data.getFragments().containsKey(key)) {
      throw new IllegalArgumentException("Local fragment not found: " + ref);
    }

    return data.getFragments().get(key);
  }
}