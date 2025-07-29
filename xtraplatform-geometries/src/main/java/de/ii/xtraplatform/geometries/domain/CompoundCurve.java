/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import com.google.common.base.Preconditions;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface CompoundCurve extends Curve<List<SingleCurve>>, CompositeGeometry<SingleCurve> {

  static CompoundCurve empty(Axes axes) {
    return ImmutableCompoundCurve.builder().axes(axes).value(List.of()).build();
  }

  static CompoundCurve of(List<SingleCurve> curves) {
    return ImmutableCompoundCurve.builder()
        .value(curves.stream().filter(ls -> !ls.isEmpty()).toList())
        .build();
  }

  static CompoundCurve of(List<SingleCurve> curves, Optional<EpsgCrs> crs) {
    return ImmutableCompoundCurve.builder()
        .crs(crs)
        .value(curves.stream().filter(ls -> !ls.isEmpty()).toList())
        .build();
  }

  @Value.Default
  default Axes getAxes() {
    if (isEmpty()) {
      return Axes.XY;
    }
    return getValue().get(0).getAxes();
  }

  @Override
  @Value.Derived
  @Value.Auxiliary
  default boolean isEmpty() {
    return getValue().isEmpty();
  }

  @Override
  @Value.Derived
  @Value.Auxiliary
  default boolean isClosed() {
    if (isEmpty()
        || getValue().get(0).getValue().getNumPositions() == 0
        || getValue().get(getValue().size() - 1).getValue().getNumPositions() == 0) {
      return true;
    }
    final int size = getAxes().size();
    final int indexLastCurve = getValue().size() - 1;
    final int numPositionsLastCurve = getValue().get(indexLastCurve).getNumPoints();
    return Arrays.equals(
        getValue().get(0).getValue().getCoordinates(),
        0,
        size,
        getValue().get(indexLastCurve).getValue().getCoordinates(),
        (numPositionsLastCurve - 1) * size,
        numPositionsLastCurve * size);
  }

  @Value.Derived
  @Value.Auxiliary
  default int getNumGeometries() {
    if (isEmpty()) {
      return 0;
    }
    return getValue().size();
  }

  @Override
  @Value.Derived
  default GeometryType getType() {
    return GeometryType.COMPOUND_CURVE;
  }

  @Value.Check
  default void check() {
    if (!isEmpty()) {
      PositionList predecessor = null;
      for (SingleCurve curve : getValue()) {
        if (curve.isEmpty()) {
          continue;
        }
        if (predecessor == null) {
          predecessor = curve.getValue();
          continue;
        }
        Preconditions.checkState(
            predecessor.get(predecessor.getNumPositions() - 1).equals(curve.getValue().get(0)),
            "The end point of the predecessor curve must match the start point of the next curve. Previous: %s, Next: %s",
            predecessor.getCoordinates(),
            curve.getValue().getCoordinates());
        predecessor = curve.getValue();
      }
    }
    Preconditions.checkArgument(
        getValue().stream().allMatch(g -> g.getAxes().equals(getAxes())),
        "All geometries must have the same axes.");
    Preconditions.checkArgument(
        getValue().stream()
            .allMatch(
                g -> (g.getCrs().isEmpty() && getCrs().isEmpty()) || (g.getCrs().equals(getCrs()))),
        "All geometries must have the same CRS.");
  }
}
