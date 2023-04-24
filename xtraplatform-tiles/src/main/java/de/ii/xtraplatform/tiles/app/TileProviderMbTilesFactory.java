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
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.features.domain.ImmutableProviderCommonData;
import de.ii.xtraplatform.store.domain.entities.AbstractEntityFactory;
import de.ii.xtraplatform.store.domain.entities.EntityData;
import de.ii.xtraplatform.store.domain.entities.EntityDataBuilder;
import de.ii.xtraplatform.store.domain.entities.EntityFactory;
import de.ii.xtraplatform.store.domain.entities.PersistentEntity;
import de.ii.xtraplatform.tiles.domain.ImmutableTileProviderMbtilesData;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetRepository;
import de.ii.xtraplatform.tiles.domain.TileProviderData;
import de.ii.xtraplatform.tiles.domain.TileProviderMbtilesData;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class TileProviderMbTilesFactory
    extends AbstractEntityFactory<TileProviderMbtilesData, TileProviderMbTiles>
    implements EntityFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileProviderMbTilesFactory.class);

  @Inject
  public TileProviderMbTilesFactory(
      // TODO: needed because dagger-auto does not parse TileProviderMbTiles
      AppContext appContext,
      TileMatrixSetRepository tileMatrixSetRepository,
      TileProviderMbTilesFactoryAssisted factoryAssisted) {
    super(factoryAssisted);
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
  public Class<? extends EntityData> dataClass() {
    return TileProviderMbtilesData.class;
  }

  @Override
  public EntityData hydrateData(EntityData entityData) {
    TileProviderMbtilesData data = (TileProviderMbtilesData) entityData;

    // TODO: auto mode

    return data;
  }

  @AssistedFactory
  public interface TileProviderMbTilesFactoryAssisted
      extends FactoryAssisted<TileProviderMbtilesData, TileProviderMbTiles> {
    @Override
    TileProviderMbTiles create(TileProviderMbtilesData data);
  }
}
