/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.google.common.collect.Range;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.tiles.domain.TileResult;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ChainedTile3dProvider {
  Logger LOGGER = LoggerFactory.getLogger(ChainedTile3dProvider.class);

  static ChainedTile3dProvider noOp() {
    return new ChainedTile3dProvider() {
      @Override
      public Map<String, Map<String, Range<Integer>>> getTmsRanges() {
        return Map.of();
      }

      @Override
      public TileResult getTile(Tile3dQuery tile) {
        return TileResult.notFound();
      }
    };
  }

  Map<String, Map<String, Range<Integer>>> getTmsRanges();

  TileResult getTile(Tile3dQuery tile) throws IOException;

  default TileResult get(Tile3dQuery tile) {
    TileResult tileResult = TileResult.notFound();

    if (canProvide(tile)) {
      try {
        tileResult = getTile(tile);
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to retrieve tile {}/{}/{}/{} for tileset '{}'. Reason: {}",
            "", // tile.getTileMatrixSet().getId(),
            tile.getLevel(),
            tile.getRow(),
            tile.getCol(),
            tile.getTileset(),
            e.getMessage());
        if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
          LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
        }
      }
    }

    if (tileResult.isNotFound() && getDelegate().isPresent()) {
      TileResult delegateResult = getDelegate().get().get(tile);

      if (!canProvide(tile)) {
        return delegateResult;
      }

      try {
        return processDelegateResult(tile, delegateResult);
      } catch (IOException e) {
        LOGGER.warn(
            "Failed to retrieve tile {}/{}/{}/{} for tileset '{}'. Reason: {}",
            "", // tile.getTileMatrixSet().getId(),
            tile.getLevel(),
            tile.getRow(),
            tile.getCol(),
            tile.getTileset(),
            e.getMessage());
        if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
          LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
        }
      }

      // delegateResult might be corrupt, recreate
      return getDelegate().get().get(tile);
    }

    return tileResult;
  }

  default Optional<ChainedTile3dProvider> getDelegate() {
    return Optional.empty();
  }

  default TileResult processDelegateResult(Tile3dQuery tile, TileResult tileResult)
      throws IOException {
    return tileResult;
  }

  default boolean canProvide(Tile3dQuery tile) {
    return true; /*getTmsRanges().containsKey(tile.getTileset())
                 && getTmsRanges().get(tile.getTileset()).containsKey(tile.getTileMatrixSet().getId())
                 && getTmsRanges()
                     .get(tile.getTileset())
                     .get(tile.getTileMatrixSet().getId())
                     .contains(tile.getLevel());*/
  }
}
