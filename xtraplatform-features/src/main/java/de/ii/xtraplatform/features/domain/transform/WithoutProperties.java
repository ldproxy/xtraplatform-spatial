/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaVisitorTopDown;
import java.util.List;
import java.util.Objects;

public class WithoutProperties implements SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {

  private final List<String> fields;
  private final boolean skipGeometry;

  public WithoutProperties(List<String> fields, boolean skipGeometry) {
    this.fields = fields.contains("*") ? List.of() : fields;
    this.skipGeometry = skipGeometry;
  }

  @Override
  public FeatureSchema visit(
      FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {

    if ((schema.isPrimaryGeometry() || schema.isSecondaryGeometry() || schema.isFilterGeometry())
        && skipGeometry) {
      return null;
    }

    if (!fields.isEmpty()
        && !parents.isEmpty()
        && !(fields.contains(getEffectiveSchema(schema, parents).getFullPathAsString())
            || visitedProperties.stream().anyMatch(Objects::nonNull))) {
      return null;
    }

    return new ImmutableFeatureSchema.Builder()
        .from(schema)
        .propertyMap(asMap(visitedProperties, FeatureSchema::getFullPathAsString))
        .build();
  }

  private FeatureSchema getEffectiveSchema(FeatureSchema schema, List<FeatureSchema> parents) {
    if (!schema.isFeatureRef() && isFeatureRef(parents)) {
      return parents.get(parents.size() - 1);
    }

    return schema;
  }

  private boolean isFeatureRef(List<FeatureSchema> parents) {
    return !parents.isEmpty() && parents.get(parents.size() - 1).isFeatureRef();
  }
}
