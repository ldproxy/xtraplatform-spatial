/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import de.ii.xtraplatform.blobs.domain.Blob;
import de.ii.xtraplatform.tiles.domain.TileResult;
import java.io.IOException;
import java.util.Optional;

public interface Tile3dStoreReadOnly {

  boolean has(Tile3dQuery tile) throws IOException;

  Optional<Blob> get(Tile3dQuery tile) throws IOException;

  boolean hasSubtree(Tile3dQuery tile) throws IOException;

  TileResult getSubtree(Tile3dQuery tile) throws IOException;

  Optional<Boolean> isEmpty(Tile3dQuery tile) throws IOException;

  boolean isEmpty() throws IOException;

  void walk(Walker walker);

  boolean has(String tileset, String tms, int level, int row, int col) throws IOException;

  @FunctionalInterface
  interface Walker {
    void walk(String tileset, String tms, int level, int row, int col);
  }
}
