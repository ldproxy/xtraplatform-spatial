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
import de.ii.xtraplatform.tiles.domain.ImmutableMinMax;
import de.ii.xtraplatform.tiles.domain.ImmutableTileProviderHttpData;
import de.ii.xtraplatform.tiles.domain.ImmutableTilesetHttpDefaults;
import de.ii.xtraplatform.tiles.domain.TileProviderData;
import de.ii.xtraplatform.tiles.domain.TileProviderHttpData;
import java.util.Optional;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class TileProviderHttpFactory
    extends AbstractEntityFactory<TileProviderHttpData, TileProviderHttp> implements EntityFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileProviderHttpFactory.class);

  private final boolean skipHydration;

  @Inject
  public TileProviderHttpFactory(TileProviderHttpFactoryAssisted factoryAssisted) {
    super(factoryAssisted);
    this.skipHydration = false;
  }

  public TileProviderHttpFactory() {
    super(null);
    this.skipHydration = true;
  }

  @Override
  public String type() {
    return TileProviderData.ENTITY_TYPE;
  }

  @Override
  public Optional<String> subType() {
    return Optional.of(TileProviderHttpData.ENTITY_SUBTYPE);
  }

  @Override
  public Class<? extends PersistentEntity> entityClass() {
    return TileProviderHttp.class;
  }

  @Override
  public EntityDataBuilder<TileProviderData> dataBuilder() {
    return new ImmutableTileProviderHttpData.Builder()
        .tilesetDefaultsBuilder(
            new ImmutableTilesetHttpDefaults.Builder()
                .putLevels(
                    "WebMercatorQuad", new ImmutableMinMax.Builder().min(0).max(23).build()));
  }

  @Override
  public EntityDataBuilder<? extends EntityData> superDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public EntityDataBuilder<TileProviderData> emptyDataBuilder() {
    return new ImmutableTileProviderHttpData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> emptySuperDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public Class<? extends EntityData> dataClass() {
    return TileProviderHttpData.class;
  }

  @Override
  public EntityData hydrateData(EntityData entityData) {
    TileProviderHttpData data = (TileProviderHttpData) entityData;

    if (skipHydration) {
      return data;
    }

    return data;
  }

  @AssistedFactory
  public interface TileProviderHttpFactoryAssisted
      extends FactoryAssisted<TileProviderHttpData, TileProviderHttp> {
    @Override
    TileProviderHttp create(TileProviderHttpData data);
  }
}
