/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.google.common.collect.Range;
import de.ii.xtraplatform.tiles.domain.Cache.Storage;
import de.ii.xtraplatform.tiles.domain.ChainedTileProvider;
import de.ii.xtraplatform.tiles.domain.GenerationParameters;
import de.ii.xtraplatform.tiles.domain.TileCache;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import de.ii.xtraplatform.tiles.domain.TileQuery;
import de.ii.xtraplatform.tiles.domain.TileResult;
import de.ii.xtraplatform.tiles.domain.TileSeedingJob;
import de.ii.xtraplatform.tiles.domain.TileSeedingJobSet;
import de.ii.xtraplatform.tiles.domain.TileStore;
import de.ii.xtraplatform.tiles.domain.TileWalker;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TileCacheDynamic implements ChainedTileProvider, TileCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileCacheDynamic.class);

  private final TileWalker tileWalker;
  private final TileStore tileStore;
  private final ChainedTileProvider delegate;
  private final Map<String, Map<String, Range<Integer>>> tmsRanges;
  private final Map<String, Map<String, Range<Integer>>> rasterTmsRanges;
  private final boolean isSeeded;

  public TileCacheDynamic(
      TileWalker tileWalker,
      TileStore tileStore,
      ChainedTileProvider delegate,
      Map<String, Map<String, Range<Integer>>> tmsRanges,
      Map<String, Map<String, Range<Integer>>> rasterTmsRanges,
      boolean seeded) {
    this.tileWalker = tileWalker;
    this.tileStore = tileStore;
    this.delegate = delegate;
    this.tmsRanges = tmsRanges;
    this.rasterTmsRanges = rasterTmsRanges;
    this.isSeeded = seeded;
  }

  @Override
  public Map<String, Map<String, Range<Integer>>> getTmsRanges() {
    return tmsRanges;
  }

  @Override
  public Optional<ChainedTileProvider> getDelegate() {
    return Optional.of(delegate);
  }

  @Override
  public TileResult getTile(TileQuery tile) throws IOException {
    if (shouldCache(tile)) {
      return tileStore.get(tile);
    }
    return TileResult.notFound();
  }

  @Override
  public TileResult processDelegateResult(TileQuery tile, TileResult tileResult)
      throws IOException {
    if (shouldCache(tile) && tileResult.isAvailable()) {
      tileStore.put(tile, new ByteArrayInputStream(tileResult.getContent().get()));

      return tileStore.get(tile);
    }

    return tileResult;
  }

  @Override
  public Map<String, Map<String, Set<TileMatrixSetLimits>>> getCoverage(
      Map<String, ? extends GenerationParameters> tilesets) throws IOException {
    return getCoverage(tilesets, tileWalker, getTmsRanges());
  }

  @Override
  public Map<String, Map<String, Set<TileMatrixSetLimits>>> getRasterCoverage(
      Map<String, ? extends GenerationParameters> tilesets) throws IOException {
    return getCoverage(tilesets, tileWalker, rasterTmsRanges);
  }

  @Override
  public Storage getStorageType() {
    return tileStore.getStorageType();
  }

  @Override
  public Optional<String> getStorageInfo(
      String tileset, String tileMatrixSet, TileMatrixSetLimits limits) {
    return tileStore.getStorageInfo(tileset, tileMatrixSet, limits);
  }

  @Override
  public boolean isSeeded() {
    return isSeeded;
  }

  @Override
  public void setupSeeding(TileSeedingJobSet jobSet, String tileSourceLabel) throws IOException {}

  @Override
  public void cleanupSeeding(TileSeedingJobSet jobSet, String tileSourceLabel) throws IOException {
    tileStore.tidyup();
  }

  @Override
  public void seed(TileSeedingJob job, String tileSourceLabel, Runnable updateProgress)
      throws IOException {
    if (!isSeeded) {
      return;
    }

    doSeed(job, tileSourceLabel, tileStore, delegate, tileWalker, updateProgress);
  }

  @Override
  public void purge(TileSeedingJob job, String tileSourceLabel) throws IOException {
    if (!isSeeded) {
      return;
    }

    tileWalker.walkTileSeedingJobLimits(
        job,
        getTmsRanges(),
        (tileset, tileMatrixSet, limits) -> {
          try {
            tileStore.delete(tileset, tileMatrixSet, limits, false);
          } catch (IOException e) {
            // ignore
            LOGGER.debug(
                "{}: error while purging cached tiles for {}, tileset {}, tile matrix {}. Reason: {}",
                TileSeedingJobSet.LABEL,
                tileSourceLabel,
                tileset,
                limits.getTileMatrix(),
                e.getMessage());
          }
        });
  }
}
