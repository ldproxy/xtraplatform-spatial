/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

public enum Axes {
  XY(2, "", 0),
  XYZ(3, " Z", 1000),
  XYM(3, " M", 2000),
  XYZM(4, " ZM", 3000);

  private final int size;
  private final String wktSuffix;
  private final int wkbShift;

  Axes(int size, String wktSuffix, int wkbShift) {
    this.size = size;
    this.wktSuffix = wktSuffix;
    this.wkbShift = wkbShift;
  }

  public int size() {
    return size;
  }

  public String getWktSuffix() {
    return wktSuffix;
  }

  public static Axes fromWkbCode(long geometryTypeCode) {
    return (geometryTypeCode >= Axes.XYZM.wkbShift)
        ? Axes.XYZM
        : (geometryTypeCode >= Axes.XYM.wkbShift)
            ? Axes.XYM
            : (geometryTypeCode >= Axes.XYZ.wkbShift) ? Axes.XYZ : Axes.XY;
  }
}
