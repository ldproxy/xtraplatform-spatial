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
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import de.ii.xtraplatform.features.domain.TypesResolver;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DefaultRolesResolver implements TypesResolver {

  @Override
  public boolean needsResolving(
      FeatureSchema property, boolean isFeature, boolean isInConcat, boolean isInCoalesce) {
    return isFeature
        && ((property.getPrimaryGeometry().isPresent()
                && property.getPrimaryGeometry().filter(FeatureSchema::isPrimaryGeometry).isEmpty())
            || (property.getPrimaryInstant().isPresent()
                && property.getPrimaryInstant().filter(FeatureSchema::isPrimaryInstant).isEmpty()));
  }

  @Override
  public FeatureSchema resolve(FeatureSchema property, List<FeatureSchema> parents) {
    Optional<FeatureSchema> primaryGeometry = property.getPrimaryGeometry();
    Optional<FeatureSchema> primaryInstant = property.getPrimaryInstant();

    ImmutableFeatureSchema.Builder builder =
        new ImmutableFeatureSchema.Builder().from(property).propertyMap(new HashMap<>());

    property
        .getPropertyMap()
        .forEach(
            (name, prop) -> {
              if (primaryGeometry.isPresent()
                  && prop.isSpatial()
                  && Objects.equals(prop.getName(), primaryGeometry.get().getName())) {
                builder.putPropertyMap(
                    name,
                    new ImmutableFeatureSchema.Builder()
                        .from(prop)
                        .role(Role.PRIMARY_GEOMETRY)
                        .build());
              } else if (primaryInstant.isPresent()
                  && prop.isTemporal()
                  && Objects.equals(prop.getName(), primaryInstant.get().getName())) {
                builder.putPropertyMap(
                    name,
                    new ImmutableFeatureSchema.Builder()
                        .from(prop)
                        .role(Role.PRIMARY_INSTANT)
                        .build());
              } else {
                builder.putPropertyMap(name, prop);
              }
            });

    return builder.build();
  }
}
