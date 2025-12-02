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
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface Tile3dSeeding {

  String CAPABILITY = "seeding";

  SeedingOptions getOptions();

  Map<String, Map<String, Set<TileMatrixSetLimits>>> getCoverage(
      Map<String, TileGenerationParameters> tilesets) throws IOException;

  void setupSeeding(Tile3dSeedingJobSet jobSet) throws IOException;

  void cleanupSeeding(Tile3dSeedingJobSet jobSet) throws IOException;
}
