/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.google.common.collect.Range;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.base.domain.resiliency.AbstractVolatileComposed;
import de.ii.xtraplatform.base.domain.resiliency.DelayedVolatile;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.base.domain.resiliency.VolatileUnavailableException;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureProviderEntity;
import de.ii.xtraplatform.tiles.domain.ImmutableTileMatrix;
import de.ii.xtraplatform.tiles.domain.ImmutableTileMatrixSetData;
import de.ii.xtraplatform.tiles.domain.ImmutableTileMatrixSetData.Builder;
import de.ii.xtraplatform.tiles.domain.ImmutableTilesBoundingBox;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dGenerator;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderFeaturesData;
import de.ii.xtraplatform.tiles3d.domain.Tileset3dFeatures;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class Tile3dGeneratorFeatures extends AbstractVolatileComposed implements Tile3dGenerator {

  private final EntityRegistry entityRegistry;
  private final Tile3dProviderFeaturesData data;
  private final Map<String, DelayedVolatile<FeatureProvider>> featureProviders;
  private final boolean async;
  private final String label;

  public Tile3dGeneratorFeatures(
      Tile3dProviderFeaturesData data,
      EntityRegistry entityRegistry,
      VolatileRegistry volatileRegistry,
      AppContext appContext,
      boolean asyncStartup) {
    super("generator", volatileRegistry, true);
    this.data = data;
    this.entityRegistry = entityRegistry;
    this.featureProviders = new LinkedHashMap<>();
    this.async = asyncStartup;
    this.label = String.format("%s v%s", appContext.getName(), appContext.getVersion());
  }

  @Override
  public void init() {
    if (async) {
      initAsync(volatileRegistry);
    } else {
      setState(State.AVAILABLE);
      // init();
    }
  }

  @Override
  public String getLabel() {
    return label;
  }

  private void initAsync(VolatileRegistry volatileRegistry) {
    onVolatileStart();

    for (Tileset3dFeatures tileset : data.getTilesets().values()) {
      String featureProviderId =
          tileset
              .mergeDefaults(data.getTilesetDefaults())
              .getFeatureProvider()
              .orElse(Tile3dProvider.clean(data.getId()));

      if (featureProviders.containsKey(featureProviderId)) {
        continue;
      }

      DelayedVolatile<FeatureProvider> delayedVolatile =
          new DelayedVolatile<>(
              volatileRegistry,
              String.format("generator.%s", featureProviderId),
              false,
              "generation");

      addSubcomponent(delayedVolatile);

      featureProviders.putIfAbsent(featureProviderId, delayedVolatile);

      entityRegistry.addEntityListener(
          FeatureProviderEntity.class,
          fp -> {
            if (Objects.equals(fp.getId(), featureProviderId)) {
              delayedVolatile.set(fp);
            }
          },
          true);
    }

    // init();

    onVolatileStarted();
  }

  /*private void init() {
    for (TilesetFeatures tileset : data.getTilesets().values()) {
      String featureProviderId =
          tileset
              .mergeDefaults(data.getTilesetDefaults())
              .getFeatureProvider()
              .orElse(TileProviderFeatures.clean(data.getId()));

      tileBuilderForProvider.putIfAbsent(
          featureProviderId,
          tileBuilders.stream()
              .sorted(Comparator.comparingInt(TileBuilder::getPriority))
              .filter(tb -> tb.isApplicable(featureProviderId))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("No applicable tile builder found")));
    }
  }*/

  private FeatureProvider getFeatureProvider(Tileset3dFeatures tileset) {
    String featureProviderId =
        tileset.getFeatureProvider().orElse(Tile3dProvider.clean(data.getId()));

    if (async) {
      DelayedVolatile<FeatureProvider> provider = featureProviders.get(featureProviderId);

      // TODO: only crs, extents, queries needed
      if (!provider.isAvailable()) {
        throw new VolatileUnavailableException(
            String.format("Feature provider with id '%s' is not available.", featureProviderId));
      }

      return provider.get();
    }

    return entityRegistry
        .getEntity(FeatureProviderEntity.class, featureProviderId)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format("Feature provider with id '%s' not found.", featureProviderId)));
  }

  @Override
  public Optional<BoundingBox> getBounds(String tilesetId) {
    Tileset3dFeatures tileset = data.getTilesets().get(tilesetId);

    if (Objects.isNull(tileset)) {
      throw new IllegalArgumentException(String.format("Unknown tileset '%s'", tilesetId));
    }

    FeatureProvider featureProvider =
        getFeatureProvider(tileset.mergeDefaults(data.getTilesetDefaults()));

    if (!featureProvider.extents().isAvailable()) {
      return Optional.empty();
    }

    String featureType = tileset.getFeatureType().orElse(tileset.getId());

    return featureProvider.extents().get().getSpatialExtent(featureType, OgcCrs.CRS84h);
  }

  @Override
  public TileMatrixSetData getTileMatrixSetData(
      String tilesetId, String tmsId, Range<Integer> levels) {
    BoundingBox boundingBox = getBounds(tilesetId).orElseThrow();
    ImmutableTileMatrixSetData.Builder builder =
        new Builder()
            .id(tmsId)
            .crs(OgcCrs.CRS84h.toUriString())
            .orderedAxes(List.of("lon", "lat"))
            .boundingBox(
                new ImmutableTilesBoundingBox.Builder()
                    .lowerLeft(
                        BigDecimal.valueOf(boundingBox.getXmin()).setScale(7, RoundingMode.HALF_UP),
                        BigDecimal.valueOf(boundingBox.getYmin()).setScale(7, RoundingMode.HALF_UP))
                    .upperRight(
                        BigDecimal.valueOf(boundingBox.getXmax()).setScale(7, RoundingMode.HALF_UP),
                        BigDecimal.valueOf(boundingBox.getYmax()).setScale(7, RoundingMode.HALF_UP))
                    .crs(OgcCrs.CRS84.toUriString())
                    .build());

    for (int level = 0; level <= levels.upperEndpoint(); level++) {
      double resolution = 360.0 / (256 * Math.pow(2, level));
      builder.addTileMatrices(
          new ImmutableTileMatrix.Builder()
              .id(String.valueOf(level))
              .tileWidth(256)
              .tileHeight(256)
              .matrixWidth((long) Math.pow(2, level))
              .matrixHeight((long) Math.pow(2, level))
              // not needed for TileWalker, but mandatory
              .scaleDenominator(BigDecimal.valueOf(0))
              .cellSize(BigDecimal.valueOf(0))
              .pointOfOrigin(new BigDecimal[] {BigDecimal.valueOf(0), BigDecimal.valueOf(0)})
              .build());
    }

    return builder.build();
  }
}
