/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.immutables.value.Value;

@Value.Immutable
public interface TileSubMatrix extends Comparable<TileSubMatrix> {

  static TileSubMatrix of(int level, int rowMin, int rowMax, int colMin, int colMax) {
    return new ImmutableTileSubMatrix.Builder()
        .level(level)
        .rowMin(rowMin)
        .rowMax(rowMax)
        .colMin(colMin)
        .colMax(colMax)
        .build();
  }

  int getLevel();

  int getRowMin();

  int getRowMax();

  int getColMin();

  int getColMax();

  @Value.Derived
  @Value.Auxiliary
  @JsonIgnore
  default long getNumberOfTiles() {
    return ((long) getRowMax() - getRowMin() + 1) * (getColMax() - getColMin() + 1);
  }

  @Value.Lazy
  @JsonIgnore
  default TileMatrixSetLimits toLimits() {
    return new ImmutableTileMatrixSetLimits.Builder()
        .tileMatrix(String.valueOf(getLevel()))
        .minTileRow(getRowMin())
        .maxTileRow(getRowMax())
        .minTileCol(getColMin())
        .maxTileCol(getColMax())
        .build();
  }

  @Value.Lazy
  @JsonIgnore
  default String asString() {
    return String.format(
        "%d/%d-%d/%d-%d", getLevel(), getRowMin(), getRowMax(), getColMin(), getColMax());
  }

  default boolean contains(TileSubMatrix other) {
    return getLevel() == other.getLevel()
        && getRowMin() <= other.getRowMin()
        && getRowMax() >= other.getRowMax()
        && getColMin() <= other.getColMin()
        && getColMax() >= other.getColMax();
  }

  default boolean canMergeWith(TileSubMatrix other) {
    return getLevel() == other.getLevel()
        && ((getRowMin() <= other.getRowMax() + 1
                && getRowMax() >= other.getRowMin() - 1
                && (getColMin() == other.getColMin() && getColMax() == other.getColMax()))
            || (getColMin() <= other.getColMax() + 1
                && getColMax() >= other.getColMin() - 1
                && (getRowMin() == other.getRowMin() && getRowMax() == other.getRowMax())));
  }

  default boolean intersects(TileSubMatrix other) {
    return getLevel() == other.getLevel()
        && getRowMin() <= other.getRowMax()
        && getRowMax() >= other.getRowMin()
        && getColMin() <= other.getColMax()
        && getColMax() >= other.getColMin();
  }

  default TileSubMatrix mergeWith(TileSubMatrix other) {
    if (!canMergeWith(other)) {
      throw new IllegalArgumentException("TileSubMatrices are not mergeable");
    }

    return new ImmutableTileSubMatrix.Builder()
        .level(getLevel())
        .rowMin(Math.min(getRowMin(), other.getRowMin()))
        .rowMax(Math.max(getRowMax(), other.getRowMax()))
        .colMin(Math.min(getColMin(), other.getColMin()))
        .colMax(Math.max(getColMax(), other.getColMax()))
        .build();
  }

  default boolean contains(int level, int row, int col) {
    return getLevel() == level
        && getRowMin() <= row
        && getRowMax() >= row
        && getColMin() <= col
        && getColMax() >= col;
  }

  @Value.Lazy
  @JsonIgnore
  default TileSubMatrix toLowerLevelSubMatrix() {
    return getLowerLevelSubMatrix(this, 1);
  }

  @Override
  default int compareTo(TileSubMatrix o) {
    if (this.getLevel() != o.getLevel()) {
      return Integer.compare(this.getLevel(), o.getLevel());
    }
    if (this.getColMin() != o.getColMin()) {
      return Integer.compare(this.getColMin(), o.getColMin());
    }
    if (this.getRowMin() != o.getRowMin()) {
      return Integer.compare(this.getRowMin(), o.getRowMin());
    }
    if (this.getColMax() != o.getColMax()) {
      return Integer.compare(this.getColMax(), o.getColMax());
    }
    if (this.getRowMax() != o.getRowMax()) {
      return Integer.compare(this.getRowMax(), o.getRowMax());
    }
    return 0;
  }

  static TileSubMatrix getLowerLevelSubMatrix(TileSubMatrix subMatrix, int levelDelta) {
    return new ImmutableTileSubMatrix.Builder()
        .level(subMatrix.getLevel() - levelDelta)
        .rowMin(subMatrix.getRowMin() / (2 * levelDelta))
        .rowMax((subMatrix.getRowMax() - 1) / (2 * levelDelta))
        .colMin(subMatrix.getColMin() / (2 * levelDelta))
        .colMax((subMatrix.getColMax() - 1) / (2 * levelDelta))
        .build();
  }
}
