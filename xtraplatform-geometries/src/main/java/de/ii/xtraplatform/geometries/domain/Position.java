/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;

@Value.Immutable
public abstract class Position {

  @NotNull
  public abstract double[] getCoordinates();

  @NotNull
  public abstract Axes getAxes();

  public static Position empty(Axes axes) {
    double[] coordinates = new double[axes.size()];
    Arrays.fill(coordinates, Double.NaN);
    return ImmutablePosition.builder().axes(axes).coordinates(coordinates).build();
  }

  public static Position of(Axes axes, double[] coordinates) {
    return ImmutablePosition.builder().axes(axes).coordinates(coordinates).build();
  }

  public static Position ofXY(double x, double y) {
    return ImmutablePosition.builder().axes(Axes.XY).coordinates(new double[] {x, y}).build();
  }

  public static Position ofXYZ(double x, double y, double z) {
    return ImmutablePosition.builder().axes(Axes.XYZ).coordinates(new double[] {x, y, z}).build();
  }

  public static Position ofXYM(double x, double y, double m) {
    return ImmutablePosition.builder().axes(Axes.XYM).coordinates(new double[] {x, y, m}).build();
  }

  public static Position ofXYZM(double x, double y, double z, double m) {
    return ImmutablePosition.builder()
        .axes(Axes.XYZM)
        .coordinates(new double[] {x, y, z, m})
        .build();
  }

  @Value.Derived
  @Value.Auxiliary
  public int size() {
    return getAxes().size();
  }

  @Value.Derived
  @Value.Auxiliary
  public boolean isEmpty() {
    return Arrays.stream(getCoordinates()).allMatch(Double::isNaN);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Position other) {
      return getAxes() == other.getAxes()
          && Arrays.equals(getCoordinates(), other.getCoordinates());
    }
    return false;
  }

  @Value.Derived
  @Value.Auxiliary
  public double x() {
    return getCoordinates()[0];
  }

  @Value.Derived
  @Value.Auxiliary
  public double y() {
    return getCoordinates()[1];
  }

  @Value.Derived
  @Value.Auxiliary
  public double z() {
    if (getAxes() == Axes.XYZ || getAxes() == Axes.XYZM) {
      return getCoordinates()[2];
    }
    return Double.NaN;
  }

  @Value.Derived
  @Value.Auxiliary
  public double m() {
    if (getAxes() == Axes.XYM) {
      return getCoordinates()[2];
    } else if (getAxes() == Axes.XYZM) {
      return getCoordinates()[3];
    }
    return Double.NaN;
  }

  @Value.Check
  public void check() {
    Preconditions.checkState(
        getCoordinates().length == getAxes().size(),
        "Position must have %d coordinates, but got %d.",
        getAxes().size(),
        getCoordinates().length);
    Preconditions.checkState(
        isEmpty() || Arrays.stream(getCoordinates()).noneMatch(Double::isNaN),
        "Position must not have NaN coordinates unless it is EMPTY.");
  }
}
