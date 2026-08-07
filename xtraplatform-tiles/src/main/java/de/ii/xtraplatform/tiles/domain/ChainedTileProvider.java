/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.google.common.collect.Range;
import de.ii.xtraplatform.base.domain.LogContext;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ChainedTileProvider {
  Logger LOGGER = LoggerFactory.getLogger(ChainedTileProvider.class);

  static ChainedTileProvider noOp() {
    return new ChainedTileProvider() {
      @Override
      public Map<String, Map<String, Range<Integer>>> getTmsRanges() {
        return Map.of();
      }

      @Override
      public TileResult getTile(TileQuery tile) {
        return TileResult.notFound();
      }
    };
  }

  Map<String, Map<String, Range<Integer>>> getTmsRanges();

  TileResult getTile(TileQuery tile) throws IOException;

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  default TileResult get(TileQuery tile) {
    TileResult tileResult = TileResult.notFound();

    if (canProvide(tile)) {
      try {
        tileResult = getTile(tile);
      } catch (IOException e) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Failed to retrieve tile {}/{}/{}/{} for tileset '{}'. Reason: {}",
              tile.getTileMatrixSet().getId(),
              tile.getLevel(),
              tile.getRow(),
              tile.getCol(),
              tile.getTileset(),
              e.getMessage());
        }
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
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Failed to retrieve tile {}/{}/{}/{} for tileset '{}'. Reason: {}",
              tile.getTileMatrixSet().getId(),
              tile.getLevel(),
              tile.getRow(),
              tile.getCol(),
              tile.getTileset(),
              e.getMessage());
        }
        if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
          LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
        }
      }

      // delegateResult might be corrupt, recreate
      return getDelegate().get().get(tile);
    }

    return tileResult;
  }

  default Optional<ChainedTileProvider> getDelegate() {
    return Optional.empty();
  }

  default TileResult processDelegateResult(TileQuery tile, TileResult tileResult)
      throws IOException {
    return tileResult;
  }

  default boolean canProvide(TileQuery tile) {
    return getTmsRanges().containsKey(tile.getTileset())
        && getTmsRanges().get(tile.getTileset()).containsKey(tile.getTileMatrixSet().getId())
        && getTmsRanges()
            .get(tile.getTileset())
            .get(tile.getTileMatrixSet().getId())
            .contains(tile.getLevel());
  }
}
