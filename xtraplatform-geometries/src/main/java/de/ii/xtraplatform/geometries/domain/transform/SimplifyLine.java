/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

import de.ii.xtraplatform.geometries.domain.PositionList.Interpolation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;

@Value.Immutable
public abstract class SimplifyLine implements CoordinatesTransformation {

  @Value.Parameter
  protected abstract double getDistanceTolerance();

  @Override
  public boolean simplifiesArcs() {
    return true;
  }

  @Override
  public double[] onCoordinates(
      double[] coordinates,
      int length,
      int dimension,
      Optional<Interpolation> interpolation,
      OptionalInt minNumberOfPositions)
      throws IOException {
    double[] simplified =
        simplify(
            coordinates,
            length / dimension,
            dimension,
            interpolation.orElse(Interpolation.LINE),
            minNumberOfPositions.orElse(2));

    if (getNext().isEmpty()) {
      return simplified;
    }

    return getNext()
        .get()
        .onCoordinates(
            simplified, simplified.length, dimension, interpolation, minNumberOfPositions);
  }

  private double[] simplify(
      double[] coordinates,
      int numberOfPositions,
      int dimension,
      Interpolation interpolation,
      int minNumberOfPositions) {

    return interpolation == Interpolation.LINE
        ? simplifyLine(coordinates, numberOfPositions, dimension, minNumberOfPositions)
        : simplifyArcs(coordinates, numberOfPositions, dimension);
  }

  private double[] simplifyArcs(double[] coordinates, int numberOfPositions, int dimension) {

    List<double[]> arcSegments = new ArrayList<>();
    for (int i = 0; i <= numberOfPositions - 2; i += 2) {
      arcSegments.add(
          ArcInterpolator.interpolateArc3Points(coordinates, i, dimension, getDistanceTolerance()));
    }

    // Flatten the list of arrays into a single array
    int totalLength =
        arcSegments.stream().mapToInt(arr -> arr.length).sum()
            - (arcSegments.size() - 1) * dimension;
    double[] result = new double[totalLength];
    // first point
    int cursor = 0;
    System.arraycopy(arcSegments.get(0), 0, result, cursor, dimension);
    cursor += dimension;

    // Skip the first point of each segment as it is already set
    for (double[] segment : arcSegments) {
      System.arraycopy(segment, dimension, result, cursor, segment.length - dimension);
      cursor += segment.length - dimension;
    }

    return result;
  }

  public double[] simplifyLine(
      double[] coordinates, int numberOfPositions, int dimension, int minNumberOfPositions) {

    if (minNumberOfPositions > 0 && numberOfPositions <= minNumberOfPositions) {
      return Arrays.copyOf(coordinates, numberOfPositions * dimension);
    }

    boolean[] keepPoints = new boolean[numberOfPositions];
    Arrays.fill(keepPoints, true);

    if (minNumberOfPositions > 2) {
      int split = Math.max(minNumberOfPositions - 1, numberOfPositions / minNumberOfPositions);

      for (int i = 0; i < numberOfPositions; i = i + split) {
        simplifySection(
            coordinates, dimension, i, Math.min(i + split - 1, numberOfPositions - 1), keepPoints);
      }
      /*
      simplifySection(points, dimension, 0, split, keepPoints);
      simplifySection(points, dimension, split, split * 2, keepPoints);
      simplifySection(points, dimension, split * 2, split * 3, keepPoints);
      simplifySection(points, dimension, split * 3, split * 4, keepPoints);
      simplifySection(points, dimension, split * 3, numberOfPoints - 1, keepPoints);
       */
    } else {
      simplifySection(coordinates, dimension, 0, numberOfPositions - 1, keepPoints);
    }

    int simplifiedLength = 0;
    for (int i = 0; i < numberOfPositions; i++) {
      if (keepPoints[i]) {
        simplifiedLength += dimension;
      }
    }

    double[] simplifiedPoints = new double[simplifiedLength];
    int cursor = 0;
    for (int i = 0; i < numberOfPositions; i++) {
      if (keepPoints[i]) {
        for (int j = i * dimension; j < i * dimension + dimension; j++) {
          simplifiedPoints[cursor++] = coordinates[j];
        }
      }
    }

    return simplifiedPoints;
  }

  private void simplifySection(
      double[] coordinates, int dimension, int start, int end, boolean[] keepPoints) {
    if ((start + 1) == end) {
      return;
    }

    double maxDistance = -1.0;
    int maxIndex = start;
    for (int i = start + 1; i < end; i++) {
      double distance =
          distance(
              coordinates[start * dimension],
              coordinates[start * dimension + 1],
              coordinates[end * dimension],
              coordinates[end * dimension + 1],
              coordinates[i * dimension],
              coordinates[i * dimension + 1]);
      if (distance > maxDistance) {
        maxDistance = distance;
        maxIndex = i;
      }
    }
    if (maxDistance <= getDistanceTolerance()) {
      for (int i = start + 1; i < end; i++) {
        keepPoints[i] = false;
      }
    } else {
      simplifySection(coordinates, dimension, start, maxIndex, keepPoints);
      simplifySection(coordinates, dimension, maxIndex, end, keepPoints);
    }
  }

  private double distance(double p1x, double p1y, double p2x, double p2y) {
    double dx = p1x - p2x;
    double dy = p1y - p2y;
    return Math.sqrt(dx * dx + dy * dy);
  }

  private double distance(double l1x, double l1y, double l2x, double l2y, double px, double py) {
    // if start = end, then just compute distance to one of the endpoints
    if (l1x == l2x && l1y == l2y) {
      return distance(l1x, l1y, px, py);
    }

    // otherwise use comp.graphics.algorithms Frequently Asked Questions method
    /*
     * (1) r = AC dot AB
     *         ---------
     *         ||AB||^2
     *
     * r has the following meaning:
     *   r=0 P = A
     *   r=1 P = B
     *   r<0 P is on the backward extension of AB
     *   r>1 P is on the forward extension of AB
     *   0<r<1 P is interior to AB
     */

    double len2 = (l2x - l1x) * (l2x - l1x) + (l2y - l1y) * (l2y - l1y);
    double r = ((px - l1x) * (l2x - l1x) + (py - l1y) * (l2y - l1y)) / len2;

    if (r <= 0.0) {
      return distance(px, py, l1x, l1y);
    }
    if (r >= 1.0) {
      return distance(px, py, l2x, l2y);
    }

    /*
     * (2) s = (Ay-Cy)(Bx-Ax)-(Ax-Cx)(By-Ay)
     *         -----------------------------
     *                    L^2
     *
     * Then the distance from C to P = |s|*L.
     *
     * This is the same calculation as {@link #distancePointLinePerpendicular}.
     * Unrolled here for performance.
     */
    double s = ((l1y - py) * (l2x - l1x) - (l1x - px) * (l2y - l1y)) / len2;
    return Math.abs(s) * Math.sqrt(len2);
  }
}
