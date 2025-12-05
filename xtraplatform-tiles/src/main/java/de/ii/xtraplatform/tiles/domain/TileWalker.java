/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.google.common.collect.Range;
import de.ii.xtraplatform.base.domain.resiliency.Volatile2;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.services.domain.TaskContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.ws.rs.core.MediaType;

public interface TileWalker extends Volatile2 {

  void walkTileSeedingJobLimits(
      TileSeedingJob job,
      Map<String, Map<String, Range<Integer>>> tmsRanges,
      LimitsVisitor limitsVisitor)
      throws IOException;

  interface TileVisitor {
    void visit(
        String tileset,
        MediaType outputFormat,
        TileMatrixSetBase tileMatrixSet,
        int level,
        int row,
        int col)
        throws IOException;
  }

  interface LimitsVisitor {
    void visit(String tileset, TileMatrixSetBase tileMatrixSet, TileMatrixSetLimits limits)
        throws IOException;
  }

  long getNumberOfTiles(
      Set<String> tilesets,
      List<MediaType> outputFormats,
      Map<String, Map<String, Range<Integer>>> tmsRanges,
      Map<String, Optional<BoundingBox>> boundingBoxes,
      TaskContext taskContext);

  void walkTilesetsAndLimits(
      Set<String> tilesets,
      Map<String, Map<String, Range<Integer>>> tmsRanges,
      Map<String, Optional<BoundingBox>> boundingBoxes,
      LimitsVisitor limitsVisitor)
      throws IOException;

  void walkTilesetsAndLimits(
      Set<String> tilesets,
      Map<String, Map<String, Range<Integer>>> tmsRanges,
      Map<String, Optional<BoundingBox>> boundingBoxes,
      Map<String, Map<String, TileMatrixSetBase>> customTileMatrixSets,
      LimitsVisitor limitsVisitor)
      throws IOException;

  void walkTileSeedingJob(
      TileSeedingJob job,
      Map<String, Map<String, Range<Integer>>> tmsRanges,
      TileVisitor tileVisitor)
      throws IOException;
}
