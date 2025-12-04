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
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public interface TileTree {

  interface TileTreeVisitor<T> {
    T visit(TileTree tileTree, List<T> childResults);
  }

  int getLevel();

  int getRow();

  int getCol();

  List<TileTree> getSubTrees();

  List<TileSubMatrix> getContent();

  @Value.Derived
  @Value.Auxiliary
  default String getCoordinates() {
    return String.format("%d/%d/%d", getLevel(), getRow(), getCol());
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
        .content(optimize(mergedContent))
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
    List<Integer> rootLevels = new ArrayList<>();

    for (int l = 0; l <= getLevel(); l += subtreeLevels) {
      rootLevels.add(l);
    }

    if (rootLevels.contains(getLevel())) {
      return this;
    }

    return getParent(this, null, rootLevels);
  }

  TileTree ROOT = of(0, 0, 0, List.of());

  static TileTree of(int level, int row, int col, List<TileTree> subTrees) {
    return new ImmutableTileTree.Builder()
        .level(level)
        .row(row)
        .col(col)
        .subTrees(subTrees)
        .build();
  }

  static TileTree from(TileMatrixSetLimits limits, int subtreeLevels) {
    int level = Integer.parseInt(limits.getTileMatrix());
    List<Integer> rootLevels = new ArrayList<>();
    Map<String, TileTree> treeRoots = new LinkedHashMap<>();

    for (int l = 0; l <= level; l += subtreeLevels) {
      rootLevels.add(l);
    }

    for (int row = limits.getMinTileRow(); row <= limits.getMaxTileRow(); row++) {
      for (int col = limits.getMinTileCol(); col <= limits.getMaxTileCol(); col++) {
        // TODO: content from limits?
        TileTree parentTree = getParentTree(level, row, col, rootLevels);
        treeRoots.compute(
            parentTree.getCoordinates(), (k, v) -> v == null ? parentTree : v.merge(parentTree));
      }
    }

    return treeRoots.getOrDefault(ROOT.getCoordinates(), ROOT);
  }

  static TileTree getParentTree(int level, int row, int col, List<Integer> rootLevels) {
    if (rootLevels.isEmpty() || level < rootLevels.get(0)) {
      throw new IllegalArgumentException(
          "rootLevels must not be empty and level must be >= the lowest root level");
    }

    TileSubMatrix content = TileSubMatrix.of(level, row, row, col, col);
    TileTree treeRoot =
        new ImmutableTileTree.Builder().level(level).row(row).col(col).addContent(content).build();
    TileTree lastParent = rootLevels.contains(level) ? treeRoot : null;

    int maxRounds = rootLevels.size();

    while (treeRoot.getLevel() > rootLevels.get(0) && maxRounds-- >= 0) {
      treeRoot = getParent(treeRoot, lastParent, rootLevels);
      if (rootLevels.contains(treeRoot.getLevel())) {
        lastParent = treeRoot;
      }
    }

    return treeRoot;
  }

  static TileTree getParent(TileTree tree, TileTree subTree, List<Integer> rootLevels) {
    TileTree treeRoot = tree;

    if (treeRoot.getLevel() == 0) {
      return treeRoot;
    }

    for (int l = treeRoot.getLevel() - 1; l >= 0; l--) {
      treeRoot = getParent(treeRoot, rootLevels);
      if (rootLevels.contains(l)) {
        return Objects.nonNull(subTree)
            ? new ImmutableTileTree.Builder().from(treeRoot).addSubTrees(subTree).build()
            : treeRoot;
      }
    }

    return treeRoot;
  }

  static TileTree getParent(TileTree tree, List<Integer> rootLevels) {
    if (tree.getLevel() == 0) {
      return tree;
    }

    int parentLevel = tree.getLevel() - 1;
    int parentRow = tree.getRow() / 2;
    int parentCol = tree.getCol() / 2;

    return new ImmutableTileTree.Builder()
        .level(parentLevel)
        .row(parentRow)
        .col(parentCol)
        .content(rootLevels.contains(tree.getLevel()) ? List.of() : tree.getContent())
        .build();
  }
}
