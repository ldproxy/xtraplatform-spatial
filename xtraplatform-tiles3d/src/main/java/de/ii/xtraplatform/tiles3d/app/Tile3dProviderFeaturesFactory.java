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
import de.ii.xtraplatform.tiles3d.domain.ImmutableTile3dProviderFeaturesData;
import de.ii.xtraplatform.tiles3d.domain.ImmutableTileset3dFeaturesDefaults;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderFeaturesData;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class Tile3dProviderFeaturesFactory
    extends AbstractEntityFactory<Tile3dProviderFeaturesData, Tile3dProviderFeatures>
    implements EntityFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dProviderFeaturesFactory.class);

  private final boolean skipHydration;

  @Inject
  public Tile3dProviderFeaturesFactory(Tile3dProviderFeaturesFactoryAssisted factoryAssisted) {
    super(factoryAssisted);
    this.skipHydration = false;
  }

  public Tile3dProviderFeaturesFactory() {
    super(null);
    this.skipHydration = true;
  }

  @Override
  public String type() {
    return Tile3dProviderData.ENTITY_TYPE;
  }

  @Override
  public Optional<String> subType() {
    return Optional.of(Tile3dProviderFeaturesData.ENTITY_SUBTYPE);
  }

  @Override
  public Class<? extends PersistentEntity> entityClass() {
    return Tile3dProviderFeatures.class;
  }

  @Override
  public EntityDataBuilder<Tile3dProviderData> dataBuilder() {
    return new ImmutableTile3dProviderFeaturesData.Builder()
        .tilesetDefaultsBuilder(new ImmutableTileset3dFeaturesDefaults.Builder());
  }

  @Override
  public EntityDataBuilder<? extends EntityData> superDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public EntityDataBuilder<Tile3dProviderData> emptyDataBuilder() {
    return new ImmutableTile3dProviderFeaturesData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> emptySuperDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public Class<? extends EntityData> dataClass() {
    return Tile3dProviderFeaturesData.class;
  }

  @Override
  public EntityData hydrateData(EntityData entityData) {
    Tile3dProviderFeaturesData data = (Tile3dProviderFeaturesData) entityData;

    if (skipHydration) {
      return data;
    }

    return data;
  }

  @AssistedFactory
  public interface Tile3dProviderFeaturesFactoryAssisted
      extends FactoryAssisted<Tile3dProviderFeaturesData, Tile3dProviderFeatures> {
    @Override
    Tile3dProviderFeatures create(Tile3dProviderFeaturesData data);
  }
}
