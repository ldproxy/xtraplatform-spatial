/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import dagger.assisted.AssistedFactory;
import de.ii.xtraplatform.entities.domain.AbstractEntityFactory;
import de.ii.xtraplatform.entities.domain.EntityData;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.entities.domain.EntityFactory;
import de.ii.xtraplatform.entities.domain.PersistentEntity;
import de.ii.xtraplatform.features.domain.ImmutableProviderCommonData;
import de.ii.xtraplatform.tiles.domain.ImmutableTileProviderMbtilesData;
import de.ii.xtraplatform.tiles.domain.TileProviderData;
import de.ii.xtraplatform.tiles.domain.TileProviderMbtilesData;
import java.util.Optional;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class TileProviderMbTilesFactory
    extends AbstractEntityFactory<TileProviderMbtilesData, TileProviderMbTiles>
    implements EntityFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileProviderMbTilesFactory.class);

  private final boolean skipHydration;

  @Inject
  public TileProviderMbTilesFactory(TileProviderMbTilesFactoryAssisted factoryAssisted) {
    super(factoryAssisted);
    this.skipHydration = false;
  }

  public TileProviderMbTilesFactory() {
    super(null);
    this.skipHydration = true;
  }

  @Override
  public String type() {
    return TileProviderData.ENTITY_TYPE;
  }

  @Override
  public Optional<String> subType() {
    return Optional.of(TileProviderMbtilesData.ENTITY_SUBTYPE);
  }

  @Override
  public Class<? extends PersistentEntity> entityClass() {
    return TileProviderMbTiles.class;
  }

  @Override
  public EntityDataBuilder<TileProviderData> dataBuilder() {
    return new ImmutableTileProviderMbtilesData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> superDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public EntityDataBuilder<TileProviderData> emptyDataBuilder() {
    return new ImmutableTileProviderMbtilesData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> emptySuperDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public Class<? extends EntityData> dataClass() {
    return TileProviderMbtilesData.class;
  }

  @Override
  public EntityData hydrateData(EntityData entityData) {
    TileProviderMbtilesData data = (TileProviderMbtilesData) entityData;

    if (skipHydration) {
      return data;
    }

    return data;
  }

  @AssistedFactory
  public interface TileProviderMbTilesFactoryAssisted
      extends FactoryAssisted<TileProviderMbtilesData, TileProviderMbTiles> {
    @Override
    TileProviderMbTiles create(TileProviderMbtilesData data);
  }
}
