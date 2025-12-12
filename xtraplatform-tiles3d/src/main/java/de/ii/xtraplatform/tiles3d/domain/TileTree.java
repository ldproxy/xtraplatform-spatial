/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.google.common.collect.Streams;
import de.ii.xtraplatform.tiles.domain.ImmutableTileSubMatrix;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import de.ii.xtraplatform.tiles3d.domain.ImmutableTileTree.Builder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.shape.fractal.MortonCode;

@Value.Immutable
public interface TileTree {

  TileTree ROOT = of(0, 0, 0);

  static TileTree of(int level, int col, int row) {
    return new ImmutableTileTree.Builder().level(level).col(col).row(row).build();
  }

  interface TileTreeVisitor<T> {
    T visit(TileTree tileTree, List<T> childResults);
  }

  int getLevel();

  int getRow();

  int getCol();

  List<TileTree> getSubTrees();

  List<TileSubMatrix> getContent();

  @Value.Default
  default int getMaxContentLevel() {
    return getLevel();
  }

  @Value.Default
  default long getNumberOfSubtrees() {
    return 0;
  }

  @Value.Default
  default long getNumberOfTiles() {
    return 0;
  }

  @Value.Lazy
  default String getCoordinates() {
    return String.format("%d/%d/%d", getLevel(), getRow(), getCol());
  }

  default <T> T accept(TileTreeVisitor<T> visitor) {
    return visitor.visit(
        this,
        getSubTrees().stream().map(child -> child.accept(visitor)).collect(Collectors.toList()));
  }

  default TileTree merge(TileTree other) {
    if (this.getLevel() != other.getLevel()
        || this.getRow() != other.getRow()
        || this.getCol() != other.getCol()) {
      throw new IllegalArgumentException("Cannot merge TileTrees with different coordinates");
    }

    Map<String, TileTree> mergedSubTrees = new LinkedHashMap<>();

    for (TileTree child : this.getSubTrees()) {
      mergedSubTrees.put(child.getCoordinates(), child);
    }

    for (TileTree child : other.getSubTrees()) {
      mergedSubTrees.compute(child.getCoordinates(), (k, v) -> v == null ? child : v.merge(child));
    }

    List<TileSubMatrix> mergedContent = merge(this.getContent(), other.getContent());

    return new ImmutableTileTree.Builder()
        .from(this)
        .subTrees(mergedSubTrees.values())
        .numberOfSubtrees(
            mergedSubTrees.size()
                + mergedSubTrees.values().stream().mapToLong(TileTree::getNumberOfSubtrees).sum())
        .numberOfTiles(mergedSubTrees.values().stream().mapToLong(TileTree::getNumberOfTiles).sum())
        .content(optimize(mergedContent))
        .maxContentLevel(Math.max(this.getMaxContentLevel(), other.getMaxContentLevel()))
        .build();
  }

  default TileSubMatrix toSubMatrix() {
    return new ImmutableTileSubMatrix.Builder()
        .level(getLevel())
        .rowMin(getRow())
        .rowMax(getRow())
        .colMin(getCol())
        .colMax(getCol())
        .build();
  }

  static TileTree fromSubMatrix(TileSubMatrix subMatrix) {
    return new ImmutableTileTree.Builder()
        .level(subMatrix.getLevel())
        .row(subMatrix.getRowMin())
        .col(subMatrix.getColMin())
        .build();
  }

  static List<TileSubMatrix> merge(List<TileSubMatrix> content, List<TileSubMatrix> otherContent) {
    return optimize(Streams.concat(content.stream(), otherContent.stream()).toList());
  }

  static List<TileSubMatrix> optimize(List<TileSubMatrix> content) {
    if (content.size() <= 1) {
      return content;
    }

    List<TileSubMatrix> mergedContent = new ArrayList<>();

    for (TileSubMatrix otherContent : content) {
      boolean handled = false;
      for (int i = 0; i < mergedContent.size(); i++) {
        if (mergedContent.get(i).contains(otherContent)) {
          handled = true;
          break;
        }
        if (mergedContent.get(i).canMergeWith(otherContent)) {
          mergedContent.set(i, mergedContent.get(i).mergeWith(otherContent));
          handled = true;
          break;
        }
      }
      if (!handled) {
        mergedContent.add(otherContent);
      }
    }

    return mergedContent;
  }

  default TileTree parent(int subtreeLevels) {
    if (getLevel() == 0) {
      return this;
    }

    return parentOf(getLevel(), subtreeLevels, getCol(), getRow());
  }

  static TileTree parentOf(TileSubMatrix tile, int subtreeLevels) {
    return parentOf(
        tile.getLevel(), tile.getLevel() % subtreeLevels, tile.getColMin(), tile.getRowMin());
  }

  static TileTree parentOf(int globalLevel, int localLevel, int col, int row) {
    int parentLevel = globalLevel - localLevel;
    int size = 1 << localLevel;
    int parentCol = col / size;
    int parentRow = row / size;

    return new ImmutableTileTree.Builder().level(parentLevel).col(parentCol).row(parentRow).build();
  }

  default int getMortonCurveIndex(int subtreeLevels) {
    return getMortonCurveIndex(subtreeLevels, getCol(), getRow());
  }

  static int getMortonCurveIndex(int localLevel, int col, int row) {
    int size = 1 << localLevel;
    int localCol = col % size;
    int localRow = row % size;

    return MortonCode.encode(localCol, localRow);
  }

  static TileTree from(TileSubMatrix contents, int subtreeLevels) {
    List<TileTree> subtrees = subtrees(subtreeLevels /* - 1*/, 0, 0, contents, subtreeLevels);

    return new ImmutableTileTree.Builder()
        .from(ROOT)
        .subTrees(subtrees)
        .numberOfSubtrees(
            subtrees.size() + subtrees.stream().mapToLong(TileTree::getNumberOfSubtrees).sum())
        .numberOfTiles(subtrees.stream().mapToLong(TileTree::getNumberOfTiles).sum())
        .content(contents.getLevel() < subtreeLevels ? List.of(contents) : List.of())
        .maxContentLevel(contents.getLevel())
        .build();
  }

  static List<TileTree> subtrees(
      int level, int row, int col, TileSubMatrix contents, int subtreeLevels) {
    if (level > contents.getLevel()) {
      return List.of();
    }

    int deltaSub = subtreeLevels;
    int deltaCon = contents.getLevel() - level;
    List<TileTree> trees = new ArrayList<>();

    int size = 1 << deltaSub;
    int sizeCon = 1 << deltaCon;
    for (int i = 0; i < size * size; i++) {
      Coordinate coords = MortonCode.decode(i);
      int r = (int) (row + coords.getY());
      int c = (int) (col + coords.getX());

      Builder builder =
          new Builder().level(level).row(r).col(c).maxContentLevel(contents.getLevel());

      if (level + deltaSub > contents.getLevel()) {
        Optional<TileSubMatrix> content =
            TileSubMatrix.of(
                    contents.getLevel(),
                    r * sizeCon,
                    (r * sizeCon) + (sizeCon) - 1,
                    c * sizeCon,
                    (c * sizeCon) + (sizeCon) - 1)
                .intersection(contents);

        if (content.isPresent()) {
          builder.addContent(content.get()).numberOfTiles(content.get().getNumberOfTiles());
        }
      } else {
        List<TileTree> children =
            subtrees(level + deltaSub, r * size, c * size, contents, subtreeLevels);
        builder
            .subTrees(children)
            .numberOfSubtrees(
                children.size() + children.stream().mapToLong(TileTree::getNumberOfSubtrees).sum())
            .numberOfTiles(children.stream().mapToLong(TileTree::getNumberOfTiles).sum());
      }

      trees.add(builder.build());
    }

    return trees;
  }
}
