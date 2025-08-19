/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transform;

import com.google.common.primitives.Doubles;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zahnen
 */
public class DouglasPeuckerLineSimplifier {

  private double[] pts;
  private boolean[] usePts;
  private final double distanceTolerance;
  private final int minPoints;

  public DouglasPeuckerLineSimplifier(double distanceTolerance, int minPoints) {
    this.distanceTolerance = distanceTolerance;
    this.minPoints = minPoints;
  }

  public double[] simplify(double[] pts) {
    return simplify(pts, pts.length / 2);
  }

  public double[] simplify(double[] pts, int numPts) {
    this.pts = pts;
    usePts = new boolean[numPts];
    for (int i = 0; i < numPts; i++) {
      usePts[i] = true;
    }

    if (minPoints == 4 && numPts > 4) {

      int split = numPts / 5;
      simplifySection(0, split);
      simplifySection(split, split * 2);
      simplifySection(split * 2, split * 3);
      simplifySection(split * 3, split * 4);
      simplifySection(split * 3, numPts - 1);
    } else if (minPoints == 4 && numPts > 3) {

      int split = numPts / 4;
      simplifySection(0, split);
      simplifySection(split, split * 2);
      simplifySection(split * 2, split * 3);
      simplifySection(split * 2, numPts - 1);
    } else {
      simplifySection(0, numPts - 1);
    }

    List<Double> coordList = new ArrayList();
    for (int i = 0; i < numPts; i++) {
      if (usePts[i]) {
        coordList.add(pts[i * 2]);
        coordList.add(pts[i * 2 + 1]);
      }
    }

    return Doubles.toArray(coordList);
  }

  private void simplifySection(int i, int j) {
    if ((i + 1) == j) {
      return;
    }

    double maxDistance = -1.0;
    int maxIndex = i;
    for (int k = i + 1; k < j; k++) {
      double distance =
          distance(
              pts[i * 2], pts[i * 2 + 1], pts[j * 2], pts[j * 2 + 1], pts[k * 2], pts[k * 2 + 1]);
      if (distance > maxDistance) {
        maxDistance = distance;
        maxIndex = k;
      }
    }
    if (maxDistance <= distanceTolerance) {
      for (int k = i + 1; k < j; k++) {
        usePts[k] = false;
      }
    } else {
      simplifySection(i, maxIndex);
      simplifySection(maxIndex, j);
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
