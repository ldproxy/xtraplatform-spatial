/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import java.util.List;

public interface TileMatrixSetBase {

  String getId();

  EpsgCrs getCrs();

  BoundingBox getBoundingBox();

  int getTileExtent();

  int getTileSize();

  TileMatrixSetLimits getLimits(int level, BoundingBox bbox);

  List<? extends TileMatrixSetLimits> getLimitsList(MinMax tileMatrixRange, BoundingBox bbox);

  BoundingBox getTileBoundingBox(int level, int col, int row);

  double getMaxAllowableOffset(int level, int row, int col);

  double getMaxAllowableOffset(int level, int row, int col, int tileExtent);

  int getTmsRow(int level, int row);
}
