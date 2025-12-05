/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.google.common.collect.Range;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.tiles.domain.Cache.Storage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface TileCache {

  Logger LOGGER = LoggerFactory.getLogger(TileCache.class);

  Map<String, Map<String, Range<Integer>>> getTmsRanges();

  default boolean canProcess(TileSeedingJob job) {
    return getTmsRanges().containsKey(job.getTileSet())
        && getTmsRanges().get(job.getTileSet()).containsKey(job.getTileMatrixSet())
        && job.getSubMatrices().stream()
            .anyMatch(
                sub ->
                    getTmsRanges()
                        .get(job.getTileSet())
                        .get(job.getTileMatrixSet())
                        .contains(sub.getLevel()));
  }

  default boolean canProcess(TileSeedingJobSet jobSet) {
    return jobSet.getTileSets().keySet().stream().allMatch(getTmsRanges()::containsKey);
  }

  boolean isSeeded();

  void setupSeeding(TileSeedingJobSet jobSet, String tileSourceLabel) throws IOException;

  void cleanupSeeding(TileSeedingJobSet jobSet, String tileSourceLabel) throws IOException;

  void seed(TileSeedingJob job, String tileSourceLabel, Runnable updateProgress) throws IOException;

  default void doSeed(
      TileSeedingJob job,
      String tileSourceLabel,
      TileStore tileStore,
      ChainedTileProvider delegate,
      TileWalker tileWalker,
      Runnable updateProgress)
      throws IOException {
    tileWalker.walkTileSeedingJob(
        job,
        getTmsRanges(),
        (tileset, encoding, tms, level, row, col) -> {
          try {
            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace(
                  String.format(
                      "currently processing -> %s, %s/%s/%s/%s, %s",
                      job.getTileSet(), job.getTileMatrixSet(), level, row, col, encoding));
            }

            TileQuery tile =
                ImmutableTileQuery.builder()
                    .tileset(job.getTileSet())
                    .mediaType(encoding)
                    .tileMatrixSet(tms)
                    .level(level)
                    .row(row)
                    .col(col)
                    .generationParameters(job.getGenerationParameters())
                    .build();

            if (job.isReseed() || tileStore.isDirty(tile) || !tileStore.has(tile)) {
              TileResult result = delegate.get(tile);

              if (shouldCache(tile) && result.isAvailable()) {
                tileStore.put(tile, new ByteArrayInputStream(result.getContent().get()));
                /*if (isEmpty[0]) {
                  isEmpty[0] = false;
                }*/
              }

              if (result.isError()) {
                LOGGER.warn(
                    "{}: processing failed -> {}, {}/{}/{}/{}, {} | {}",
                    tileSourceLabel,
                    job.getTileSet(),
                    tms.getId(),
                    level,
                    row,
                    col,
                    encoding,
                    result.getError().get());
              }
            }
          } finally {
            updateProgress.run();
          }
        });
  }

  default void purge(TileSeedingJob job, String tileSourceLabel) throws IOException {}

  Map<String, Map<String, Set<TileMatrixSetLimits>>> getCoverage(
      Map<String, TileGenerationParameters> tilesets) throws IOException;

  Map<String, Map<String, Set<TileMatrixSetLimits>>> getRasterCoverage(
      Map<String, TileGenerationParameters> tilesets) throws IOException;

  Storage getStorageType();

  Optional<String> getStorageInfo(String tileset, String tileMatrixSet, TileMatrixSetLimits limits);

  default Map<String, Map<String, Set<TileMatrixSetLimits>>> getCoverage(
      Map<String, TileGenerationParameters> tilesets,
      TileWalker tileWalker,
      Map<String, Map<String, Range<Integer>>> tmsRanges)
      throws IOException {
    Map<String, Optional<BoundingBox>> boundingBoxes = getBoundingBoxes(tilesets);
    Map<String, Map<String, Set<TileMatrixSetLimits>>> coverage = new LinkedHashMap<>();

    tileWalker.walkTilesetsAndLimits(
        tilesets.keySet(),
        tmsRanges,
        boundingBoxes,
        (tileset, tms, limits) -> {
          if (!coverage.containsKey(tileset)) {
            coverage.put(tileset, new LinkedHashMap<>());
          }
          if (!coverage.get(tileset).containsKey(tms.getId())) {
            coverage.get(tileset).put(tms.getId(), new LinkedHashSet<>());
          }
          coverage.get(tileset).get(tms.getId()).add(limits);
        });

    return coverage;
  }

  default Map<String, Map<String, Set<TileMatrixSetLimits>>> getCoverage(
      Map<String, TileGenerationParameters> tilesets,
      TileWalker tileWalker,
      Map<String, Map<String, Range<Integer>>> tmsRanges,
      Map<String, Map<String, TileMatrixSetBase>> customTileMatrixSets)
      throws IOException {
    Map<String, Optional<BoundingBox>> boundingBoxes = getBoundingBoxes(tilesets);
    Map<String, Map<String, Set<TileMatrixSetLimits>>> coverage = new LinkedHashMap<>();

    tileWalker.walkTilesetsAndLimits(
        tilesets.keySet(),
        tmsRanges,
        boundingBoxes,
        customTileMatrixSets,
        (tileset, tms, limits) -> {
          if (!coverage.containsKey(tileset)) {
            coverage.put(tileset, new LinkedHashMap<>());
          }
          if (!coverage.get(tileset).containsKey(tms.getId())) {
            coverage.get(tileset).put(tms.getId(), new LinkedHashSet<>());
          }
          coverage.get(tileset).get(tms.getId()).add(limits);
        });

    return coverage;
  }

  default Map<String, Optional<BoundingBox>> getBoundingBoxes(
      Map<String, TileGenerationParameters> tilesets) {
    return tilesets.entrySet().stream()
        .map(
            entry ->
                new SimpleImmutableEntry<>(entry.getKey(), entry.getValue().getClipBoundingBox()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  default boolean shouldCache(TileQuery tileQuery) {
    return !tileQuery.isTransient();
  }
}
