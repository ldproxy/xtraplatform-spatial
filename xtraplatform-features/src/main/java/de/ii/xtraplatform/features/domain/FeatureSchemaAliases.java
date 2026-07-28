/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.transform.ImmutablePropertyTransformation;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformation;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FeatureSchemaAliases {

  private FeatureSchemaAliases() {}

  public static PropertyTransformations injectAliasRenames(
      PropertyTransformations propertyTransformations, FeatureSchema schema) {
    Map<String, List<PropertyTransformation>> aliasRenames = new LinkedHashMap<>();
    collectAliasRenames(schema, aliasRenames);
    if (aliasRenames.isEmpty()) {
      return propertyTransformations;
    }
    return propertyTransformations.mergeInto(() -> aliasRenames);
  }

  private static void collectAliasRenames(
      FeatureSchema schema, Map<String, List<PropertyTransformation>> aliasRenames) {
    schema
        .getAlias()
        .filter(alias -> !schema.getFullPath().isEmpty())
        .ifPresent(
            alias ->
                aliasRenames.put(
                    schema.getFullPathAsString(),
                    List.of(new ImmutablePropertyTransformation.Builder().rename(alias).build())));
    schema.getProperties().forEach(child -> collectAliasRenames(child, aliasRenames));
  }
}
