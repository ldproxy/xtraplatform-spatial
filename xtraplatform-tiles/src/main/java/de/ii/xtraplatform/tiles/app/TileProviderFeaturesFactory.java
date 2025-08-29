/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableMap;
import dagger.assisted.AssistedFactory;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.entities.domain.AbstractEntityFactory;
import de.ii.xtraplatform.entities.domain.AutoEntity;
import de.ii.xtraplatform.entities.domain.AutoEntityFactory;
import de.ii.xtraplatform.entities.domain.EntityData;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.entities.domain.EntityFactory;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.entities.domain.PersistentEntity;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureProviderEntity;
import de.ii.xtraplatform.features.domain.ImmutableProviderCommonData;
import de.ii.xtraplatform.tiles.domain.ImmutableMinMax;
import de.ii.xtraplatform.tiles.domain.ImmutableTileProviderFeaturesData;
import de.ii.xtraplatform.tiles.domain.ImmutableTilesetFeatures;
import de.ii.xtraplatform.tiles.domain.ImmutableTilesetFeatures.Builder;
import de.ii.xtraplatform.tiles.domain.ImmutableTilesetFeaturesDefaults;
import de.ii.xtraplatform.tiles.domain.TileProviderData;
import de.ii.xtraplatform.tiles.domain.TileProviderFeaturesData;
import de.ii.xtraplatform.tiles.domain.TilesetFeatures;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class TileProviderFeaturesFactory
    extends AbstractEntityFactory<TileProviderFeaturesData, TileProviderFeatures>
    implements EntityFactory, AutoEntityFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileProviderFeaturesFactory.class);

  private final EntityRegistry entityRegistry;
  private final boolean skipHydration;

  @Inject
  public TileProviderFeaturesFactory(
      EntityRegistry entityRegistry, TileProviderFeaturesFactoryAssisted factoryAssisted) {
    super(factoryAssisted);
    this.entityRegistry = entityRegistry;
    this.skipHydration = false;
  }

  // for ldproxy-cfg
  public TileProviderFeaturesFactory() {
    super(null);
    this.entityRegistry = null;
    this.skipHydration = true;
  }

  @Override
  public String type() {
    return TileProviderData.ENTITY_TYPE;
  }

  @Override
  public Optional<String> subType() {
    return Optional.of(TileProviderFeaturesData.ENTITY_SUBTYPE);
  }

  @Override
  public Class<? extends PersistentEntity> entityClass() {
    return TileProviderFeatures.class;
  }

  @Override
  public EntityDataBuilder<TileProviderData> dataBuilder() {
    return new ImmutableTileProviderFeaturesData.Builder()
        .tilesetDefaultsBuilder(
            new ImmutableTilesetFeaturesDefaults.Builder()
                .putLevels("WebMercatorQuad", new ImmutableMinMax.Builder().min(0).max(23).build())
                .featureLimit(100000)
                .minimumSizeInPixel(0.5)
                .ignoreInvalidGeometries(false)
                .sparse(false));
  }

  @Override
  public EntityDataBuilder<? extends EntityData> superDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public EntityDataBuilder<TileProviderData> emptyDataBuilder() {
    return new ImmutableTileProviderFeaturesData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> emptySuperDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public Class<? extends EntityData> dataClass() {
    return TileProviderFeaturesData.class;
  }

  @Override
  public EntityData hydrateData(EntityData entityData) {
    TileProviderFeaturesData data = (TileProviderFeaturesData) entityData;

    if (skipHydration) {
      return data;
    }

    if (data.isAuto()) {
      LOGGER.info(
          "Provider with id '{}' is in auto mode, generating configuration ...", data.getId());

      try {
        data = generateDefaultsIfNecessary(generateTilesetsIfNecessary(data));

        data =
            new ImmutableTileProviderFeaturesData.Builder()
                .from(data)
                .auto(Optional.empty())
                .build();
      } catch (Throwable e) {
        if (LOGGER.isErrorEnabled()) {
          LogContext.error(LOGGER, e, "Provider with id '{}' could not be started", data.getId());
        }
        throw e;
      }
    }

    return data;
  }

  private TileProviderFeaturesData generateDefaultsIfNecessary(TileProviderFeaturesData data) {
    if (!data.getTilesetDefaults().getLevels().isEmpty()) {
      return data;
    }

    return new ImmutableTileProviderFeaturesData.Builder()
        .from(data)
        .tilesetDefaults(
            new ImmutableTilesetFeaturesDefaults.Builder()
                .from(data.getTilesetDefaults())
                .levels(
                    ImmutableMap.of(
                        "WebMercatorQuad", new ImmutableMinMax.Builder().min(0).max(23).build()))
                .build())
        .build();
  }

  private TileProviderFeaturesData generateTilesetsIfNecessary(TileProviderFeaturesData data) {
    if (!data.getTilesets().isEmpty()) {
      return data;
    }

    String featureProviderId =
        data.getTilesetDefaults()
            .getFeatureProvider()
            .orElse(TileProviderFeatures.clean(data.getId()));
    FeatureProvider featureProvider =
        entityRegistry
            .getEntity(FeatureProviderEntity.class, featureProviderId)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Feature provider with id '%s' not found.", featureProviderId)));

    if (!featureProvider.queries().isSupported()) {
      throw new IllegalStateException("Feature provider has no Queries support.");
    }
    if (!featureProvider.crs().isSupported()) {
      throw new IllegalStateException("Feature provider has no CRS support.");
    }

    TilesetFeatures all =
        new Builder().id("__all__").addCombine(TilesetFeatures.COMBINE_ALL).build();

    Map<String, TilesetFeatures> tilesets =
        featureProvider.info().getSchemas().stream()
            .filter(featureSchema -> featureSchema.getPrimaryGeometry().isPresent())
            .map(
                featureSchema -> {
                  TilesetFeatures tilesetFeatures =
                      new Builder().id(featureSchema.getName()).build();

                  return new SimpleEntry<>(featureSchema.getName(), tilesetFeatures);
                })
            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

    if (tilesets.isEmpty()) {
      return data;
    }

    return new ImmutableTileProviderFeaturesData.Builder()
        .from(data)
        .putTilesets(all.getId(), all)
        .putAllTilesets(tilesets)
        .build();
  }

  @Override
  public <T extends AutoEntity> Map<String, String> check(T entityData) {
    return Map.of();
  }

  @Override
  public <T extends AutoEntity> Map<String, List<String>> analyze(T entityData) {
    return Map.of();
  }

  @Override
  public <T extends AutoEntity> T generate(
      T entityData, Map<String, List<String>> types, Consumer<Map<String, List<String>>> tracker) {
    if (!(entityData instanceof TileProviderFeaturesData)) {
      return entityData;
    }

    TileProviderFeaturesData data = (TileProviderFeaturesData) entityData;

    Map<String, TilesetFeatures> collections =
        types.values().stream()
            .flatMap(Collection::stream)
            .map(
                type -> {
                  ImmutableTilesetFeatures collection = new Builder().id(type).build();

                  return new SimpleImmutableEntry<>(type, collection);
                })
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

    return (T)
        new ImmutableTileProviderFeaturesData.Builder().from(data).tilesets(collections).build();
  }

  @AssistedFactory
  public interface TileProviderFeaturesFactoryAssisted
      extends FactoryAssisted<TileProviderFeaturesData, TileProviderFeatures> {
    @Override
    TileProviderFeatures create(TileProviderFeaturesData data);
  }
}
