/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import de.ii.xtraplatform.base.domain.resiliency.OptionalVolatileCapability;
import de.ii.xtraplatform.base.domain.resiliency.VolatileComposed;
import de.ii.xtraplatform.entities.domain.PersistentEntity;
import de.ii.xtraplatform.features.domain.FeatureProvider.FeatureVolatileCapability;

public interface Tile3dProvider extends PersistentEntity, VolatileComposed {

  String STORE_DIR_NAME = "3dtiles";

  @Override
  Tile3dProviderData getData();

  @Override
  default String getType() {
    return Tile3dProviderData.ENTITY_TYPE;
  }

  default OptionalVolatileCapability<Tile3dAccess> access() {
    return new FeatureVolatileCapability<>(Tile3dAccess.class, Tile3dAccess.CAPABILITY, this);
  }

  default OptionalVolatileCapability<TileSeeding> seeding() {
    return new FeatureVolatileCapability<>(TileSeeding.class, TileSeeding.CAPABILITY, this);
  }
}
