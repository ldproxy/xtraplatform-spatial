/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

public final class BoundingBoxStrings {

  private final String xmin;
  private final String ymin;
  private final String xmax;
  private final String ymax;

  public BoundingBoxStrings(String xmin, String ymin, String xmax, String ymax) {
    this.xmin = xmin;
    this.ymin = ymin;
    this.xmax = xmax;
    this.ymax = ymax;
  }

  public String getXmin() {
    return xmin;
  }

  public String getYmin() {
    return ymin;
  }

  public String getXmax() {
    return xmax;
  }

  public String getYmax() {
    return ymax;
  }
}
