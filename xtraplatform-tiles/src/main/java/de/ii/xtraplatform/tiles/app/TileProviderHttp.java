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
import de.ii.xtraplatform.entities.domain.Entity;
import de.ii.xtraplatform.entities.domain.Entity.SubType;
import de.ii.xtraplatform.features.domain.ProviderData;
import de.ii.xtraplatform.tiles.domain.ChainedTileProvider;
import de.ii.xtraplatform.tiles.domain.ImmutableTilesetMetadata;
import de.ii.xtraplatform.tiles.domain.TileAccess;
import de.ii.xtraplatform.tiles.domain.TileProvider;
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
public class TileProviderHttp extends AbstractTileProvider<TileProviderHttpData>
    implements TileProvider, TileAccess {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileProviderHttp.class);
  private final ChainedTileProvider providerChain;
  private final Map<String, TilesetMetadata> metadata;

  @AssistedInject
  public TileProviderHttp(VolatileRegistry volatileRegistry, @Assisted TileProviderHttpData data) {
    super(volatileRegistry, data, "access");
    this.metadata = new LinkedHashMap<>();

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

    addStaticCapability("access", "Tile Access");

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
    return ImmutableTilesetMetadata.builder()
        .encodings(
            tileset.getEncodings().isEmpty()
                ? getData().getTilesetDefaults().getEncodings().keySet()
                : tileset.getEncodings().keySet())
        .levels(
            tileset.getLevels().isEmpty()
                ? getData().getTilesetDefaults().getLevels()
                : tileset.getLevels())
        .center(tileset.getCenter().or(() -> getData().getTilesetDefaults().getCenter()))
        .build();
  }
}
