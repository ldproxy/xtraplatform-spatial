/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import de.ii.xtraplatform.tiles.domain.SeedingOptions;
import de.ii.xtraplatform.tiles.domain.TileGenerationParameters;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetBase;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import de.ii.xtraplatform.tiles.domain.TileSeedingJob;
import de.ii.xtraplatform.tiles.domain.TileSeedingJobSet;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import java.io.IOException;
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
}
