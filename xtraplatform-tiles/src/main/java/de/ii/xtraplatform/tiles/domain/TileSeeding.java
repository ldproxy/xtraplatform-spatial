/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public interface TileSeeding {

  String CAPABILITY = "seeding";

  SeedingOptions getOptions();

  Map<String, Map<String, Set<TileMatrixSetLimits>>> getCoverage(
      Map<String, TileGenerationParameters> tilesets) throws IOException;

  Map<String, Map<String, Set<TileMatrixSetLimits>>> getRasterCoverage(
      Map<String, TileGenerationParameters> tilesets) throws IOException;

  Map<String, String> getRasterStorageInfo(
      String rasterTileset, String tileMatrixSet, TileSubMatrix subMatrix);

  Map<String, String> getRasterStorageInfo(
      String rasterTileset,
      String tileMatrixSet,
      TileSubMatrix subMatrix,
      String vectorTileset,
      TileSubMatrix vectorSubMatrix);

  void setupSeeding(TileSeedingJobSet jobSet) throws IOException;

  void cleanupSeeding(TileSeedingJobSet jobSet) throws IOException;

  void runSeeding(TileSeedingJob job, Consumer<Integer> updateProgress) throws IOException;

  default void deleteFromCache(
      String tileset, TileMatrixSetBase tileMatrixSet, TileMatrixSetLimits limits) {}

  static void mergeCoverageInto(
      Map<String, Map<String, Set<TileMatrixSetLimits>>> source,
      Map<String, Map<String, Set<TileMatrixSetLimits>>> target) {
    source.forEach(
        (tileset, tms) -> {
          if (!target.containsKey(tileset)) {
            target.put(tileset, new LinkedHashMap<>());
          }
          tms.forEach(
              (tmsId, limits) -> {
                if (!target.get(tileset).containsKey(tmsId)) {
                  target.get(tileset).put(tmsId, new LinkedHashSet<>());
                }
                target.get(tileset).get(tmsId).addAll(limits);
              });
        });
  }
}
