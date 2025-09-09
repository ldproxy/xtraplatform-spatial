/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;

@Value.Immutable
public abstract class PositionList {

  public enum Interpolation {
    LINE,
    CIRCULAR
  }

  public static PositionList empty(Axes axes) {
    return ImmutablePositionList.builder().axes(axes).coordinates(new double[0]).build();
  }

  public static PositionList of(Axes axes, double[] coordinates) {
    return ImmutablePositionList.builder().axes(axes).coordinates(coordinates).build();
  }

  public static PositionList of(Axes axes, double[] coordinates, Interpolation interpolation) {
    return ImmutablePositionList.builder()
        .axes(axes)
        .coordinates(coordinates)
        .interpolation(interpolation)
        .build();
  }

  public static PositionList of(List<Position> positions) {
    if (positions.isEmpty()) {
      return empty(Axes.XY);
    }
    Axes axes = positions.get(0).getAxes();
    double[] coordinates = new double[axes.size() * positions.size()];
    for (int i = 0; i < positions.size(); i++) {
      Position position = positions.get(i);
      System.arraycopy(
          position.getCoordinates(),
          0,
          coordinates,
          i * position.getAxes().size(),
          position.getAxes().size());
    }
    return PositionList.of(axes, coordinates);
  }

  public static PositionList concat(PositionList first, PositionList second) {
    if (first.getAxes() != second.getAxes()) {
      throw new IllegalArgumentException(
          String.format(
              "Axes of both PositionLists must match. Found: %s, %s.",
              first.getAxes(), second.getAxes()));
    }
    if (first.getInterpolation() != second.getInterpolation()) {
      throw new IllegalArgumentException(
          String.format(
              "Interpolation of both PositionLists must match. Found: %s, %s.",
              first.getInterpolation(), second.getInterpolation()));
    }
    double[] p1 = first.get(first.getNumPositions() - 1).getCoordinates();
    double[] p2 = second.get(0).getCoordinates();
    if (!IntStream.range(0, first.getAxes().size()).allMatch(i -> p1[i] == p2[i])) {
      throw new IllegalArgumentException(
          String.format(
              "PositionLists must be continuous. Found: %s, %s.",
              Arrays.toString(first.getCoordinates()), Arrays.toString(second.getCoordinates())));
    }
    double[] coordinates =
        new double
            [first.getCoordinates().length
                + second.getCoordinates().length
                - first.getAxes().size()];
    System.arraycopy(first.getCoordinates(), 0, coordinates, 0, first.getCoordinates().length);
    System.arraycopy(
        second.getCoordinates(),
        second.getAxes().size(),
        coordinates,
        first.getCoordinates().length,
        second.getCoordinates().length - second.getAxes().size());
    return ImmutablePositionList.builder()
        .axes(first.getAxes())
        .coordinates(coordinates)
        .interpolation(first.getInterpolation())
        .build();
  }

  @NotNull
  public abstract double[] getCoordinates();

  @NotNull
  public abstract Axes getAxes();

  @Value.Default
  public Interpolation getInterpolation() {
    return Interpolation.LINE;
  }

  @Value.Derived
  @Value.Auxiliary
  public int getNumPositions() {
    return getCoordinates().length / getAxes().size();
  }

  @Value.Derived
  @Value.Auxiliary
  public boolean isEmpty() {
    return getCoordinates().length == 0;
  }

  @Value.Lazy
  public PositionList reverse() {
    int dimension = getAxes().size();
    int length = getCoordinates().length;
    double[] coordinates = Arrays.copyOf(getCoordinates(), length);
    for (int i = 0; i < length / 2; i = i + dimension) {
      double x = coordinates[i];
      double y = coordinates[i + 1];

      int newXPos = length - 1 - i - (dimension - 1);
      int newYPos = newXPos + 1;

      coordinates[i] = coordinates[newXPos];
      coordinates[i + 1] = coordinates[newYPos];
      coordinates[newXPos] = x;
      coordinates[newYPos] = y;

      if (dimension >= 3) {
        double z = coordinates[i + 2];
        int newZPos = newYPos + 1;
        coordinates[i + 2] = coordinates[newZPos];
        coordinates[newZPos] = z;
      }

      if (dimension == 4) {
        double m = coordinates[i + 3];
        int newMPos = newYPos + 2;
        coordinates[i + 3] = coordinates[newMPos];
        coordinates[newMPos] = m;
      }
    }
    return PositionList.of(getAxes(), coordinates, getInterpolation());
  }

  public Position get(int index) {
    if (index < 0 || index >= getNumPositions()) {
      throw new IndexOutOfBoundsException("Index out of bounds: " + index);
    }
    double[] coordinates = new double[getAxes().size()];
    System.arraycopy(getCoordinates(), index * getAxes().size(), coordinates, 0, getAxes().size());
    return Position.of(getAxes(), coordinates);
  }

  public PositionList subList(int fromIndex, int toIndex) {
    if (fromIndex < 0 || fromIndex >= getNumPositions()) {
      throw new IndexOutOfBoundsException("Index out of bounds: " + fromIndex);
    }
    if (toIndex <= fromIndex || toIndex >= getNumPositions()) {
      throw new IndexOutOfBoundsException("Index out of bounds: " + toIndex);
    }
    double[] coordinates = new double[(toIndex - fromIndex) * getAxes().size()];
    System.arraycopy(
        getCoordinates(),
        fromIndex * getAxes().size(),
        coordinates,
        0,
        (toIndex - fromIndex) * getAxes().size());
    return PositionList.of(getAxes(), coordinates);
  }
}
