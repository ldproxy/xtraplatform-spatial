/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.domain;

public abstract class BoundingBoxTransformer implements CrsTransformer {

  @Override
  public BoundingBox transformBoundingBox(BoundingBox boundingBox)
      throws CrsTransformationException {
    return boundingBox.is3d()
        ? transformBoundingBox3D(boundingBox)
        : transformBoundingBox2D(boundingBox);
  }

  private BoundingBox transformBoundingBox2D(BoundingBox boundingBox)
      throws CrsTransformationException {

    CoordinateTuple ll = this.transform(boundingBox.getXmin(), boundingBox.getYmin());
    CoordinateTuple lr = this.transform(boundingBox.getXmax(), boundingBox.getYmin());
    CoordinateTuple ul = this.transform(boundingBox.getXmin(), boundingBox.getYmax());
    CoordinateTuple ur = this.transform(boundingBox.getXmax(), boundingBox.getYmax());

    if (ll.isNull() || ul.isNull() || lr.isNull() || ur.isNull()) {
      throw new CrsTransformationException(
          String.format(
              "Failed to transform bounding box corner point coordinate(s): %s", boundingBox));
    }

    // The corner points of the bounding box are transformed into the system crsOut.
    // Build the bounding box in crsOut from the min/max values of the edge.
    final double xmin = Math.min(ul.getX(), ll.getX());
    final double ymin = Math.min(lr.getY(), ll.getY());
    final double xmax = Math.max(ur.getX(), lr.getX());
    final double ymax = Math.max(ul.getY(), ur.getY());

    return BoundingBox.of(xmin, ymin, xmax, ymax, getTargetCrs());
  }

  @SuppressWarnings("PMD.CyclomaticComplexity")
  private BoundingBox transformBoundingBox3D(BoundingBox boundingBox)
      throws CrsTransformationException {
    assert boundingBox.is3d();

    double[] llb =
        this.transform(
            new double[] {boundingBox.getXmin(), boundingBox.getYmin(), boundingBox.getZmin()},
            1,
            3);
    double[] llt =
        this.transform(
            new double[] {boundingBox.getXmin(), boundingBox.getYmin(), boundingBox.getZmax()},
            1,
            3);
    double[] lrb =
        this.transform(
            new double[] {boundingBox.getXmax(), boundingBox.getYmin(), boundingBox.getZmin()},
            1,
            3);
    double[] lrt =
        this.transform(
            new double[] {boundingBox.getXmax(), boundingBox.getYmin(), boundingBox.getZmax()},
            1,
            3);
    double[] ulb =
        this.transform(
            new double[] {boundingBox.getXmin(), boundingBox.getYmax(), boundingBox.getZmin()},
            1,
            3);
    double[] ult =
        this.transform(
            new double[] {boundingBox.getXmin(), boundingBox.getYmax(), boundingBox.getZmax()},
            1,
            3);
    double[] urb =
        this.transform(
            new double[] {boundingBox.getXmax(), boundingBox.getYmax(), boundingBox.getZmin()},
            1,
            3);
    double[] urt =
        this.transform(
            new double[] {boundingBox.getXmax(), boundingBox.getYmax(), boundingBox.getZmax()},
            1,
            3);

    if (llb == null
        || llt == null
        || lrb == null
        || lrt == null
        || ulb == null
        || ult == null
        || urb == null
        || urt == null) {
      throw new CrsTransformationException(
          String.format(
              "Failed to transform bounding box corner point coordinate(s): %s", boundingBox));
    }

    // The corner points of the bounding box are transformed into the system crsOut.
    // Build the bounding box in crsOut from the min/max values.
    final double xmin = min(llb[0], llt[0], ulb[0], ult[0]);
    final double ymin = min(llb[1], llt[1], lrb[1], lrt[1]);
    final double zmin = min(llb[2], lrb[2], ulb[2], urb[2]);
    final double xmax = max(lrb[0], lrt[0], urb[0], urt[0]);
    final double ymax = max(ulb[1], ult[1], urb[1], urt[1]);
    final double zmax = max(llt[2], lrt[2], ult[2], urt[2]);

    return BoundingBox.of(xmin, ymin, zmin, xmax, ymax, zmax, getTargetCrs());
  }

  private static double min(double... vals) {
    assert vals.length > 0;
    double ret = Double.MAX_VALUE;
    for (double val : vals) {
      if (val < ret) {
        ret = val;
      }
    }
    return ret;
  }

  private static double max(double... vals) {
    assert vals.length > 0;
    double ret = -Double.MAX_VALUE;
    for (double val : vals) {
      if (val > ret) {
        ret = val;
      }
    }
    return ret;
  }
}
