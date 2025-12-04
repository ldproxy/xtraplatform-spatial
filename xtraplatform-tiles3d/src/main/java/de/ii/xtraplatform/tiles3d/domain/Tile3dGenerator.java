/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.google.common.collect.Range;
import de.ii.xtraplatform.base.domain.resiliency.VolatileComposed;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetBase;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetData;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import de.ii.xtraplatform.tiles3d.domain.spec.Subtree;
import java.util.List;
import java.util.Optional;

public interface Tile3dGenerator extends VolatileComposed {

  String CAPABILITY = "generation";

  void init();

  String getLabel();

  Optional<BoundingBox> getBounds(String tilesetId);

  TileMatrixSetData getTileMatrixSetData(String tilesetId, String tmsId, Range<Integer> levels);

  Subtree generateSubtree(String tilesetId, TileTree subtree);

  List<Tile3dCoordinates> getAvailableTiles(
      Tileset3dFeatures tileset, TileTree parent, TileSubMatrix tileSubMatrix, Subtree subtree);

  byte[] generateTile(
      Tileset3dFeatures tileset,
      Tile3dCoordinates tile,
      Tile3dSeedingJob job,
      Tile3dSeedingJobSet jobSet,
      TileMatrixSetBase tileMatrixSet);

  byte[] subtreeToBinary(Subtree subtree);

  Subtree subtreeFromBinary(byte[] subtreeBytes);
}
