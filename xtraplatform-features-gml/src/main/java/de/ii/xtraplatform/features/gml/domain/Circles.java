/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

/**
 * Helpers for converting between {@code <gml:Circle>} (3 control points on a circle) and the
 * 5-position closed CIRCULARSTRING representation used internally. The internal form satisfies
 * {@code isClosed()} (first position equals last) so it passes ring-closure validation and is
 * accepted as a full circle by PostGIS as {@code CIRCULARSTRING(P1, P2, P3, antipode(P2), P1)}.
 *
 * <p>Only 2D (XY) circles are handled; 3D circles would require resolving the circle's plane in
 * 3-space and are not produced by current data.
 */
final class Circles {

  private Circles() {}

  /** Coordinate-unit tolerance for the {@link #isFullCircleClosed(double[])} check. */
  private static final double EPS = 1.0e-6;

  /** Determinant tolerance below which three points are treated as colinear. */
  private static final double COLINEAR_EPS = 1.0e-12;

  /**
   * Expand the 3 control points of a {@code <gml:Circle>} into a 5-position closed CIRCULARSTRING:
   * {@code (P1, P2, P3, antipode(P2), P1)}.
   *
   * @param xyP1P2P3 6 doubles: {@code x1, y1, x2, y2, x3, y3}
   * @return 10 doubles: the 5 expanded positions
   * @throws IllegalArgumentException if the three points are colinear (no circle is defined)
   */
  static double[] expandCircleToClosed(double[] xyP1P2P3) {
    if (xyP1P2P3.length != 6) {
      throw new IllegalArgumentException(
          "expected exactly 3 XY positions (6 doubles), got " + xyP1P2P3.length);
    }
    double x1 = xyP1P2P3[0];
    double y1 = xyP1P2P3[1];
    double x2 = xyP1P2P3[2];
    double y2 = xyP1P2P3[3];
    double x3 = xyP1P2P3[4];
    double y3 = xyP1P2P3[5];
    double[] c = circumcenter(x1, y1, x2, y2, x3, y3);
    double x4 = 2.0 * c[0] - x2;
    double y4 = 2.0 * c[1] - y2;
    return new double[] {x1, y1, x2, y2, x3, y3, x4, y4, x1, y1};
  }

  /**
   * Test whether 5 closed positions form a full circle: positions 1, 2, 3 define a circle, the
   * first and fifth positions coincide, and the fourth position is the antipode of the second
   * position on that circle (all within {@link #EPS}).
   *
   * @param xy5positions 10 doubles: {@code x1, y1, x2, y2, x3, y3, x4, y4, x5, y5}
   */
  static boolean isFullCircleClosed(double[] xy5positions) {
    if (xy5positions.length != 10) {
      return false;
    }
    double x1 = xy5positions[0];
    double y1 = xy5positions[1];
    double x5 = xy5positions[8];
    double y5 = xy5positions[9];
    if (Math.abs(x1 - x5) > EPS || Math.abs(y1 - y5) > EPS) {
      return false;
    }
    double x2 = xy5positions[2];
    double y2 = xy5positions[3];
    double x3 = xy5positions[4];
    double y3 = xy5positions[5];
    double x4 = xy5positions[6];
    double y4 = xy5positions[7];
    try {
      double[] c = circumcenter(x1, y1, x2, y2, x3, y3);
      double expectedX4 = 2.0 * c[0] - x2;
      double expectedY4 = 2.0 * c[1] - y2;
      return Math.abs(x4 - expectedX4) <= EPS && Math.abs(y4 - expectedY4) <= EPS;
    } catch (IllegalArgumentException colinear) {
      return false;
    }
  }

  /**
   * Circumcenter of three non-colinear 2D points.
   *
   * @throws IllegalArgumentException if the three points are colinear
   */
  static double[] circumcenter(double x1, double y1, double x2, double y2, double x3, double y3) {
    double d = 2.0 * (x1 * (y2 - y3) + x2 * (y3 - y1) + x3 * (y1 - y2));
    if (Math.abs(d) < COLINEAR_EPS) {
      throw new IllegalArgumentException("three points are colinear; no circle is defined");
    }
    double s1 = x1 * x1 + y1 * y1;
    double s2 = x2 * x2 + y2 * y2;
    double s3 = x3 * x3 + y3 * y3;
    double cx = (s1 * (y2 - y3) + s2 * (y3 - y1) + s3 * (y1 - y2)) / d;
    double cy = (s1 * (x3 - x2) + s2 * (x1 - x3) + s3 * (x2 - x1)) / d;
    return new double[] {cx, cy};
  }
}
