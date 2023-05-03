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
import de.ii.xtraplatform.tiles.domain.ChainedTileProvider;
import de.ii.xtraplatform.tiles.domain.ImmutableTilesetMetadata;
import de.ii.xtraplatform.tiles.domain.TileProvider;
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

public class TileProviderHttp extends AbstractTileProvider<TileProviderHttpData>
    implements TileProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileProviderHttp.class);
  private final ChainedTileProvider providerChain;
  private final Map<String, TilesetMetadata> metadata;

  @AssistedInject
  public TileProviderHttp(@Assisted TileProviderHttpData data) {
    super(data);
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

    return super.onStartup();
  }

  @Override
  public Optional<TilesetMetadata> metadata(String tileset) {
    return Optional.ofNullable(metadata.get(tileset));
  }

  @Override
  public TileResult getTile(TileQuery tile) {
    Optional<TileResult> error = validate(tile);

    if (error.isPresent()) {
      return error.get();
    }

    return providerChain.get(tile);
  }

  @Override
  public boolean supportsGeneration() {
    return false;
  }

  @Override
  public String getType() {
    return TileProviderHttpData.PROVIDER_TYPE;
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
