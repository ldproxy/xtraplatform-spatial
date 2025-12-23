/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

import java.util.ArrayList;
import java.util.List;

public class ArcInterpolator {

  public static double[] interpolateArcString(
      double[] coordinates, int dimension, double maxDeviation) {
    int numberOfPositions = coordinates.length / dimension;
    List<double[]> arcSegments = new ArrayList<>();
    for (int i = 0; i <= numberOfPositions - 2; i += 2) {
      arcSegments.add(
          ArcInterpolator.interpolateArc3Points(coordinates, i, dimension, maxDeviation));
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

  public static double[] interpolateArc3Points(
      double[] coordinates, int index, int dimension, double maxDeviation) {
    double[] center2d = calculateCircleCenter2d(coordinates, index, dimension);

    if (Double.valueOf(center2d[0]).isNaN() || Double.valueOf(center2d[1]).isNaN()) {
      // Points are collinear, return a line with the first and last points
      double[] result = new double[2 * dimension];
      System.arraycopy(coordinates, index * dimension, result, 0, dimension);
      System.arraycopy(coordinates, (index + 2) * dimension, result, dimension, dimension);
      return result;
    }

    double radius =
        distance2d(
            center2d,
            new double[] {coordinates[index * dimension], coordinates[index * dimension + 1]});

    double angle1 =
        Math.atan2(
            coordinates[index * dimension + 1] - center2d[1],
            coordinates[index * dimension] - center2d[0]);
    double angle2 =
        Math.atan2(
            coordinates[(index + 1) * dimension + 1] - center2d[1],
            coordinates[(index + 1) * dimension] - center2d[0]);
    double angle3 =
        Math.atan2(
            coordinates[(index + 2) * dimension + 1] - center2d[1],
            coordinates[(index + 2) * dimension] - center2d[0]);

    boolean isClockwise =
        (angle2 - angle1 + 2 * Math.PI) % (2 * Math.PI)
            > (angle3 - angle1 + 2 * Math.PI) % (2 * Math.PI);
    ;

    double startAngle = angle1;
    double endAngle = angle3;

    double startZ = dimension == 3 ? coordinates[index * dimension + 2] : 0.0;
    double endZ = dimension == 3 ? coordinates[(index + 2) * dimension + 2] : 0.0;

    if (!isClockwise) {
      double temp = startAngle;
      startAngle = endAngle;
      endAngle = temp;
      temp = startZ;
      startZ = endZ;
      endZ = temp;
    }

    if (startAngle < endAngle) {
      startAngle += 2 * Math.PI;
    }

    // Start with four points and increase if necessary
    int numPoints = 4;

    while (true) {
      double[] interpolatedPoints = new double[numPoints * dimension];
      double step = (endAngle - startAngle) / (numPoints - 1);
      double stepZ = (endZ - startZ) / (numPoints - 1);

      // Compute the points along the arc
      for (int i = 0; i < numPoints; i++) {
        double angle = startAngle + i * step;
        interpolatedPoints[i * dimension] = center2d[0] + radius * Math.cos(angle);
        interpolatedPoints[i * dimension + 1] = center2d[1] + radius * Math.sin(angle);
        if (dimension == 3) {
          interpolatedPoints[i * dimension + 2] = startZ + i * stepZ;
        }
      }

      // Compute the maximum deviation from the arc
      double maxError = calculateMaxDeviation(interpolatedPoints, dimension, center2d, radius);
      if (maxError <= maxDeviation) {
        if (!isClockwise) {
          // Reverse the order of points if the arc is counter-clockwise
          for (int i = 0; i < numPoints / 2; i++) {
            for (int j = 0; j < dimension; j++) {
              double tempCoord = interpolatedPoints[i * dimension + j];
              interpolatedPoints[i * dimension + j] =
                  interpolatedPoints[(numPoints - 1 - i) * dimension + j];
              interpolatedPoints[(numPoints - 1 - i) * dimension + j] = tempCoord;
            }
          }
        }
        // Ensure the first and last points are the original coordinates without rounding effects
        interpolatedPoints[0] = coordinates[index * dimension];
        interpolatedPoints[1] = coordinates[index * dimension + 1];
        if (dimension == 3) {
          interpolatedPoints[2] = coordinates[index * dimension + 2];
        }
        interpolatedPoints[(numPoints - 1) * dimension] = coordinates[(index + 2) * dimension];
        interpolatedPoints[(numPoints - 1) * dimension + 1] =
            coordinates[(index + 2) * dimension + 1];
        if (dimension == 3) {
          interpolatedPoints[(numPoints - 1) * dimension + 2] =
              coordinates[(index + 2) * dimension + 2];
        }
        return interpolatedPoints;
      }

      numPoints *= 2; // double the number of points for finer resolution
    }
  }

  private static double calculateMaxDeviation(
      double[] coordinates, int dimension, double[] center2d, double radius) {
    double maxDeviation = 0.0;

    for (int i = 0; i < coordinates.length / dimension - 1; i++) {
      // compute the midpoint of the segment
      double midX = (coordinates[i * dimension] + coordinates[(i + 1) * dimension]) / 2;
      double midY = (coordinates[i * dimension + 1] + coordinates[(i + 1) * dimension + 1]) / 2;

      // Compute the distance from the midpoint to the circle center
      double distanceToCenter = distance2d(new double[] {midX, midY}, center2d);
      double deviation = Math.abs(distanceToCenter - radius);

      // Update the maximum deviation
      maxDeviation = Math.max(maxDeviation, deviation);
    }

    return maxDeviation;
  }

  private static double distance2d(double[] point1, double[] point2) {
    return Math.sqrt(Math.pow(point1[0] - point2[0], 2) + Math.pow(point1[1] - point2[1], 2));
  }

  private static double[] calculateCircleCenter2d(double[] coordinates, int index, int dimension) {
    double x1 = coordinates[index * dimension], y1 = coordinates[index * dimension + 1];
    double x2 = coordinates[(index + 1) * dimension], y2 = coordinates[(index + 1) * dimension + 1];
    double x3 = coordinates[(index + 2) * dimension], y3 = coordinates[(index + 2) * dimension + 1];

    // Center points of the segments
    double mx1 = (x1 + x2) / 2.0, my1 = (y1 + y2) / 2.0;
    double mx2 = (x2 + x3) / 2.0, my2 = (y2 + y3) / 2.0;

    // Gradients of the segments
    double dx1 = x2 - x1, dy1 = y2 - y1;
    double dx2 = x3 - x2, dy2 = y3 - y2;

    // Handle vertical/horizontal lines to avoid NaN values
    boolean vert1 = dx1 == 0.0;
    boolean vert2 = dx2 == 0.0;
    boolean hor1 = dy1 == 0.0;
    boolean hor2 = dy2 == 0.0;

    if ((vert1 && vert2) || (hor1 && hor2)) {
      // collinear points, not an arc
      return new double[] {Double.NaN, Double.NaN};
    } else if (vert1) {
      // first segment is vertical
      double perp2 = -dx2 / dy2;
      double centerY = my1;
      double centerX = ((centerY - my2) / perp2) + mx2;
      return new double[] {centerX, centerY};
    } else if (vert2) {
      // second segment is vertical
      double perp1 = -dx1 / dy1;
      double centerY = my2;
      double centerX = ((centerY - my1) / perp1) + mx1;
      return new double[] {centerX, centerY};
    } else if (hor1) {
      // first segment is horizontal
      double perp2 = -dx2 / dy2;
      double centerX = mx1;
      double centerY = perp2 * (centerX - mx2) + my2;
      return new double[] {centerX, centerY};
    } else if (hor2) {
      // second segment is horizontal
      double perp1 = -dx1 / dy1;
      double centerX = mx2;
      double centerY = perp1 * (centerX - mx1) + my1;
      return new double[] {centerX, centerY};
    } else {
      // standard case
      double perp1 = -dx1 / dy1;
      double perp2 = -dx2 / dy2;
      if (perp1 == perp2) {
        return new double[] {Double.NaN, Double.NaN};
      }
      double centerX = (perp1 * mx1 - perp2 * mx2 + my2 - my1) / (perp1 - perp2);
      double centerY = perp1 * (centerX - mx1) + my1;
      return new double[] {centerX, centerY};
    }
  }
}
