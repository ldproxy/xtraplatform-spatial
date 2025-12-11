/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import java.io.IOException;

public interface Tile3dStore extends Tile3dStoreReadOnly {

  boolean hasSubtree(int level, int x, int y) throws IOException;

  byte[] getSubtree(int level, int x, int y) throws IOException;

  void putSubtree(int level, int x, int y, byte[] subtree) throws IOException;

  boolean hasContent(int level, int x, int y) throws IOException;

  void putContent(int level, int x, int y, byte[] tile) throws IOException;
}
