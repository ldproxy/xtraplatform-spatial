/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.google.common.collect.Range;
import de.ii.xtraplatform.tiles.domain.Cache.Storage;
import de.ii.xtraplatform.tiles.domain.TileCache;
import de.ii.xtraplatform.tiles.domain.TileGenerationParameters;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetBase;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import de.ii.xtraplatform.tiles.domain.TileSeedingJob;
import de.ii.xtraplatform.tiles.domain.TileSeedingJobSet;
import de.ii.xtraplatform.tiles.domain.TileWalker;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Tile3dCacheDynamic implements TileCache {

  private final TileWalker tileWalker;
  private final Map<String, Map<String, TileMatrixSetBase>> customTileMatrixSets;
  private final Map<String, Map<String, Range<Integer>>> tmsRanges;

  public Tile3dCacheDynamic(
      TileWalker tileWalker,
      Map<String, Map<String, TileMatrixSetBase>> customTileMatrixSets,
      Map<String, Map<String, Range<Integer>>> tmsRanges) {
    this.tileWalker = tileWalker;
    this.customTileMatrixSets = customTileMatrixSets;
    this.tmsRanges = tmsRanges;
  }

  @Override
  public Map<String, Map<String, Range<Integer>>> getTmsRanges() {
    return tmsRanges;
  }

  @Override
  public boolean isSeeded() {
    return true;
  }

  @Override
  public void setupSeeding(TileSeedingJobSet jobSet, String tileSourceLabel) throws IOException {}

  @Override
  public void cleanupSeeding(TileSeedingJobSet jobSet, String tileSourceLabel) throws IOException {}

  @Override
  public void seed(TileSeedingJob job, String tileSourceLabel, Runnable updateProgress)
      throws IOException {}

  @Override
  public Map<String, Map<String, Set<TileMatrixSetLimits>>> getCoverage(
      Map<String, TileGenerationParameters> tilesets) throws IOException {
    return getCoverage(tilesets, tileWalker, getTmsRanges(), customTileMatrixSets);
  }

  @Override
  public Map<String, Map<String, Set<TileMatrixSetLimits>>> getRasterCoverage(
      Map<String, TileGenerationParameters> tilesets) throws IOException {
    return Map.of();
  }

  @Override
  public Storage getStorageType() {
    return Storage.PER_TILE;
  }

  @Override
  public Optional<String> getStorageInfo(
      String tileset, String tileMatrixSet, TileMatrixSetLimits limits) {
    return Optional.empty();
  }
}
