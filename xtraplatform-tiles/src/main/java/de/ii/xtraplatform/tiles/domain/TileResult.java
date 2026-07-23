/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.google.common.base.Preconditions;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface TileResult {

  enum Status {
    // Tile is available from the tile provider
    FOUND,
    // Like FOUND, but the tile is empty (no data) and tiles at more detailed zoom levels
    // are guaranteed to be empty, too
    EMPTY,
    // Like FOUND, but the tile has data and tiles at more detailed zoom levels are guaranteed
    // to be identical
    FULL,
    // Tile is not available from the provider
    NOT_FOUND,
    // Tile is outside the tile matrix set limits
    OUTSIDE_LIMITS,
    // Not a valid tile
    ERROR
  }

  TileResult NOT_FOUND = new ImmutableTileResult.Builder().status(Status.NOT_FOUND).build();

  static TileResult notFound() {
    return NOT_FOUND;
  }

  static TileResult notFound(byte[] content) {
    return new ImmutableTileResult.Builder().status(Status.NOT_FOUND).content(content).build();
  }

  static TileResult empty(byte[] content) {
    return new ImmutableTileResult.Builder().status(Status.EMPTY).content(content).build();
  }

  static TileResult full(byte[] content) {
    return new ImmutableTileResult.Builder().status(Status.FULL).content(content).build();
  }

  static TileResult found(byte[] content) {
    return new ImmutableTileResult.Builder().status(Status.FOUND).content(content).build();
  }

  static TileResult outsideLimits(String message) {
    return new ImmutableTileResult.Builder().status(Status.OUTSIDE_LIMITS).error(message).build();
  }

  static TileResult error(String message) {
    return new ImmutableTileResult.Builder().status(Status.ERROR).error(message).build();
  }

  Status getStatus();

  Optional<byte[]> getContent();

  Optional<String> getError();

  @Value.Derived
  default boolean isAvailable() {
    return getContent().isPresent();
  }

  @Value.Derived
  default boolean isEmpty() {
    return getStatus() == Status.EMPTY;
  }

  @Value.Derived
  default boolean isFull() {
    return getStatus() == Status.FULL;
  }

  @Value.Derived
  default boolean isNotFound() {
    return getStatus() == Status.NOT_FOUND;
  }

  @Value.Derived
  default boolean isOutsideLimits() {
    return getStatus() == Status.OUTSIDE_LIMITS;
  }

  @Value.Derived
  default boolean isError() {
    return getStatus() == Status.ERROR && getError().isPresent();
  }

  @Value.Check
  default void check() {
    if (getStatus() == Status.FOUND) {
      Preconditions.checkState(getContent().isPresent(), "content is required for status 'FOUND'");
    } else if (getStatus() == Status.EMPTY) {
      Preconditions.checkState(getContent().isPresent(), "content is required for status 'EMPTY'");
    } else if (getStatus() == Status.FULL) {
      Preconditions.checkState(getContent().isPresent(), "content is required for status 'FULL'");
    } else if (getStatus() == Status.OUTSIDE_LIMITS) {
      Preconditions.checkState(
          getError().isPresent(), "error is required for status 'OUTSIDE_LIMITS'");
    } else if (getStatus() == Status.ERROR) {
      Preconditions.checkState(getError().isPresent(), "error is required for status 'ERROR'");
    }
  }
}
