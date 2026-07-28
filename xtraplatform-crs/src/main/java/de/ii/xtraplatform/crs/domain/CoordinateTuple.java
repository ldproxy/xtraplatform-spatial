/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.domain;

/**
 * @author fischer
 */
@SuppressWarnings({
  "PMD.UseVarargs",
  "PMD.ConstructorCallsOverridableMethod",
  "PMD.ArrayIsStoredDirectly",
  "PMD.MethodReturnsInternalArray"
})
public class CoordinateTuple {

  protected double[] c;
  protected boolean[] used;

  public CoordinateTuple() {
    this.c = new double[2];
    this.used = new boolean[2];
  }

  public CoordinateTuple(double x, double y) {
    this();
    setX(x);
    setY(y);
  }

  public CoordinateTuple(String x, String y) {
    this(Double.parseDouble(x), Double.parseDouble(y));
  }

  public CoordinateTuple(double[] c) {
    this();
    this.c = c;
    this.used[0] = true;
    this.used[1] = true;
  }

  public double[] asArray() {
    return c;
  }

  public double getX() {
    return c[0];
  }

  public double getY() {
    return c[1];
  }

  public void setX(double x) {
    this.c[0] = x;
    this.used[0] = true;
  }

  public void setY(double y) {
    this.c[1] = y;
    this.used[1] = true;
  }

  public String getXasString() {
    return String.valueOf(c[0]);
  }

  public String getYasString() {
    return String.valueOf(c[1]);
  }

  public boolean hasX() {
    return used[0];
  }

  public boolean hasY() {
    return used[1];
  }

  public boolean isNull() {
    return c == null;
  }
}
