/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.google.common.collect.Range;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedInject;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.CrsTransformationException;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.entities.domain.Entity;
import de.ii.xtraplatform.entities.domain.Entity.SubType;
import de.ii.xtraplatform.features.domain.ProviderData;
import de.ii.xtraplatform.tiles.domain.ChainedTileProvider;
import de.ii.xtraplatform.tiles.domain.ImmutableTilesetMetadata;
import de.ii.xtraplatform.tiles.domain.MinMax;
import de.ii.xtraplatform.tiles.domain.TileMatrixSet;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetRepository;
import de.ii.xtraplatform.tiles.domain.TileProviderData;
import de.ii.xtraplatform.tiles.domain.TileProviderHttpData;
import de.ii.xtraplatform.tiles.domain.TileQuery;
import de.ii.xtraplatform.tiles.domain.TileResult;
import de.ii.xtraplatform.tiles.domain.TileStoreReadOnly;
import de.ii.xtraplatform.tiles.domain.TilesetHttp;
import de.ii.xtraplatform.tiles.domain.TilesetMetadata;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Entity(
    type = TileProviderData.ENTITY_TYPE,
    subTypes = {
      @SubType(key = ProviderData.PROVIDER_TYPE_KEY, value = TileProviderData.PROVIDER_TYPE),
      @SubType(
          key = ProviderData.PROVIDER_SUB_TYPE_KEY,
          value = TileProviderHttpData.PROVIDER_SUBTYPE)
    },
    data = TileProviderHttpData.class)
public class TileProviderHttp extends AbstractTileProvider<TileProviderHttpData> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileProviderHttp.class);
  private final ChainedTileProvider providerChain;
  private final Map<String, TilesetMetadata> metadata;
  private final TileMatrixSetRepository tileMatrixSetRepository;
  private final CrsTransformerFactory crsTransformerFactory;

  @AssistedInject
  public TileProviderHttp(
      TileMatrixSetRepository tileMatrixSetRepository,
      CrsTransformerFactory crsTransformerFactory,
      VolatileRegistry volatileRegistry,
      @Assisted TileProviderHttpData data) {
    super(volatileRegistry, data, "access");
    this.metadata = new LinkedHashMap<>();
    this.tileMatrixSetRepository = tileMatrixSetRepository;
    this.crsTransformerFactory = crsTransformerFactory;

    Map<String, String> tilesetSources =
        data.getTilesets().entrySet().stream()
            .map(
                entry ->
                    new SimpleImmutableEntry<>(entry.getKey(), entry.getValue().getUrlTemplate()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    TileStoreReadOnly tileStore = new TileStoreHttp(tilesetSources);

    this.providerChain =
        new ChainedTileProvider() {
          @Override
          public Map<String, Map<String, Range<Integer>>> getTmsRanges() {
            return data.getTmsRanges();
          }

          @Override
          public TileResult getTile(TileQuery tile) throws IOException {
            return tileStore.get(tile);
          }
        };
  }

  @Override
  protected boolean onStartup() throws InterruptedException {

    loadMetadata();

    onVolatileStart();

    addCapability("access", true);

    return true;
  }

  @Override
  public Optional<TilesetMetadata> getMetadata(String tilesetId) {
    return Optional.ofNullable(metadata.get(tilesetId));
  }

  @Override
  public TileResult getTile(TileQuery tile) {
    Optional<TileResult> error = validate(tile);

    if (error.isPresent()) {
      return error.get();
    }

    return providerChain.get(tile);
  }

  private void loadMetadata() {
    getData()
        .getTilesets()
        .forEach(
            (key, tileset) -> {
              metadata.put(key, loadMetadata(tileset));
            });
  }

  private TilesetMetadata loadMetadata(TilesetHttp tileset) {
    Map<String, MinMax> levels =
        tileset.getLevels().isEmpty()
            ? getData().getTilesetDefaults().getLevels()
            : tileset.getLevels();
    Optional<BoundingBox> bounds =
        tileset
            .getExtent()
            .or(() -> getData().getTilesetDefaults().getExtent())
            .flatMap(extent -> extent.toBoundingBox(OgcCrs.CRS84))
            .or(() -> getTileMatrixSetsExtent(levels.keySet()));

    return ImmutableTilesetMetadata.builder()
        .encodings(
            tileset.getEncodings().isEmpty()
                ? getData().getTilesetDefaults().getEncodings().keySet()
                : tileset.getEncodings().keySet())
        .levels(levels)
        .center(tileset.getCenter().or(() -> getData().getTilesetDefaults().getCenter()))
        .bounds(bounds)
        .build();
  }

  private Optional<BoundingBox> getTileMatrixSetsExtent(Set<String> tileMatrixSetIds) {
    return tileMatrixSetIds.stream()
        .map(tileMatrixSetRepository::get)
        .flatMap(Optional::stream)
        .map(this::toCrs84)
        .flatMap(Optional::stream)
        .reduce(BoundingBox::merge);
  }

  private Optional<BoundingBox> toCrs84(TileMatrixSet tileMatrixSet) {
    try {
      return Optional.of(tileMatrixSet.getBoundingBoxCrs84(crsTransformerFactory));
    } catch (CrsTransformationException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Could not transform the extent of tile matrix set '{}' to CRS84: {}",
            tileMatrixSet.getId(),
            e.getMessage());
      }
      return Optional.empty();
    }
  }
}
