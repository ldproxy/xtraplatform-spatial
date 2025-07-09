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
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder;
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
    boolean feature =
        isFeature
            && ((property.getPrimaryGeometry().isPresent()
                    && property
                        .getPrimaryGeometry()
                        .filter(FeatureSchema::isPrimaryGeometry)
                        .isEmpty())
                || (property.getPrimaryInstant().isPresent()
                    && property
                        .getPrimaryInstant()
                        .filter(FeatureSchema::isPrimaryInstant)
                        .isEmpty()));
    boolean embedded =
        property.isEmbeddedFeature()
            && ((property.getEmbeddedPrimaryGeometry().isPresent()
                    && property
                        .getEmbeddedPrimaryGeometry()
                        .filter(FeatureSchema::isEmbeddedPrimaryGeometry)
                        .isEmpty())
                || (property.getEmbeddedPrimaryInstant().isPresent()
                    && property
                        .getEmbeddedPrimaryInstant()
                        .filter(FeatureSchema::isEmbeddedPrimaryInstant)
                        .isEmpty()));
    return feature || embedded;
  }

  @Override
  public FeatureSchema resolve(FeatureSchema property, List<FeatureSchema> parents) {
    boolean isEmbedded = property.isEmbeddedFeature();
    Optional<FeatureSchema> primaryGeometry =
        isEmbedded ? property.getEmbeddedPrimaryGeometry() : property.getPrimaryGeometry();
    Optional<FeatureSchema> primaryInstant =
        isEmbedded ? property.getEmbeddedPrimaryInstant() : property.getPrimaryInstant();

    ImmutableFeatureSchema.Builder builder =
        new ImmutableFeatureSchema.Builder().from(property).propertyMap(new HashMap<>());

    property
        .getPropertyMap()
        .forEach(
            (name, prop) -> {
              if (primaryGeometry.isPresent()
                  && prop.isSpatial()
                  && Objects.equals(prop.getName(), primaryGeometry.get().getName())) {
                Builder builder2 = new Builder().from(prop);
                if (isEmbedded) {
                  builder2.embeddedRole(Role.PRIMARY_GEOMETRY);
                } else {
                  builder2.role(Role.PRIMARY_GEOMETRY);
                }
                builder.putPropertyMap(name, builder2.build());
              } else if (primaryInstant.isPresent()
                  && prop.isTemporal()
                  && Objects.equals(prop.getName(), primaryInstant.get().getName())) {
                Builder builder2 = new Builder().from(prop);
                if (isEmbedded) {
                  builder2.embeddedRole(Role.PRIMARY_INSTANT);
                } else {
                  builder2.role(Role.PRIMARY_INSTANT);
                }
                builder.putPropertyMap(name, builder2.build());
              } else {
                builder.putPropertyMap(name, prop);
              }
            });

    return builder.build();
  }
}
