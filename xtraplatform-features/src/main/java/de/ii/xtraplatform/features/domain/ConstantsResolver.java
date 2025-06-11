/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.List;

public class ConstantsResolver implements TypesResolver {
  private final int[] constantCounter;

  public ConstantsResolver() {
    this.constantCounter = new int[] {0};
  }

  @Override
  public boolean needsResolving(
      FeatureSchema property, boolean isFeature, boolean isInConcat, boolean isInCoalesce) {
    return property.getConstantValue().isPresent() && property.getSourcePaths().isEmpty();
  }

  @Override
  public FeatureSchema resolve(FeatureSchema schema, List<FeatureSchema> parents) {
    String parentName =
        parents.isEmpty()
            ? "root"
            : parents.get(parents.size() - 1).getName().replaceAll("[^a-zA-Z0-9]", "_");
    String constantValue =
        schema.getType() == SchemaBase.Type.STRING
            ? String.format("'%s'", schema.getConstantValue().get())
            : schema.getConstantValue().get();
    String constantSourcePath =
        String.format(
            "%sconstant_%s_%d{constant=%s}",
            schema.getSourcePath().orElse(""), parentName, constantCounter[0]++, constantValue);

    return new ImmutableFeatureSchema.Builder().from(schema).sourcePath(constantSourcePath).build();
  }
}
