/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import dagger.assisted.AssistedFactory;
import de.ii.xtraplatform.entities.domain.AbstractEntityFactory;
import de.ii.xtraplatform.entities.domain.EntityData;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.entities.domain.EntityFactory;
import de.ii.xtraplatform.entities.domain.PersistentEntity;
import de.ii.xtraplatform.features.domain.ImmutableProviderCommonData;
import de.ii.xtraplatform.tiles3d.domain.ImmutableTile3dProviderFilesData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderFilesData;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
@AutoBind
public class Tile3dProviderFilesFactory
    extends AbstractEntityFactory<Tile3dProviderFilesData, Tile3dProviderFiles>
    implements EntityFactory {

  private final boolean skipHydration;

  @Inject
  public Tile3dProviderFilesFactory(Tile3dProviderFilesFactoryAssisted factoryAssisted) {
    super(factoryAssisted);
    this.skipHydration = false;
  }

  public Tile3dProviderFilesFactory() {
    super(null);
    this.skipHydration = true;
  }

  @Override
  public String type() {
    return Tile3dProviderData.ENTITY_TYPE;
  }

  @Override
  public Optional<String> subType() {
    return Optional.of(Tile3dProviderFilesData.ENTITY_SUBTYPE);
  }

  @Override
  public Class<? extends PersistentEntity> entityClass() {
    return Tile3dProviderFiles.class;
  }

  @Override
  public EntityDataBuilder<Tile3dProviderData> dataBuilder() {
    return new ImmutableTile3dProviderFilesData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> superDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public EntityDataBuilder<Tile3dProviderData> emptyDataBuilder() {
    return new ImmutableTile3dProviderFilesData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> emptySuperDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public Class<? extends EntityData> dataClass() {
    return Tile3dProviderFilesData.class;
  }

  @Override
  public EntityData hydrateData(EntityData entityData) {
    Tile3dProviderFilesData data = (Tile3dProviderFilesData) entityData;

    if (skipHydration) {
      return data;
    }

    return data;
  }

  @AssistedFactory
  public interface Tile3dProviderFilesFactoryAssisted
      extends FactoryAssisted<Tile3dProviderFilesData, Tile3dProviderFiles> {
    @Override
    Tile3dProviderFiles create(Tile3dProviderFilesData data);
  }
}
