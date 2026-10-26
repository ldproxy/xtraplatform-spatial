/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public interface Tile3dSeeding {

  String CAPABILITY = "seeding";

  SeedingOptions3d getOptions();

  Map<String, Map<String, Set<TileMatrixSetLimits>>> getCoverage(
      Map<String, Tile3dGenerationParameters> tilesets) throws IOException;

  void setupSeeding(Tile3dSeedingJob job) throws IOException;

  void cleanupSeeding(Tile3dSeedingJob job) throws IOException;

  void seedSubtrees(Tile3dSeedingPartialJob job, Consumer<Integer> updateProgress)
      throws IOException;

  void seedTiles(
      Tile3dSeedingPartialJob job, Tile3dSeedingJob seedingJob, Consumer<Integer> updateProgress)
      throws IOException;
}
