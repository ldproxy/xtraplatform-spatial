/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

public enum GmlType {
  ID("ID"),
  STRING("string"),
  DATE_TIME("dateTime"),
  DATE("date"),
  GEOMETRY("geometry"),
  DECIMAL("decimal"),
  DOUBLE("double"),
  FLOAT("float"),
  INT("int"),
  INTEGER("integer"),
  LONG("long"),
  SHORT("short"),
  BOOLEAN("boolean"),
  URI("anyURI"),
  NONE("");

  private final String stringRepresentation;

  GmlType(String stringRepresentation) {
    this.stringRepresentation = stringRepresentation;
  }

  @Override
  public String toString() {
    return stringRepresentation;
  }

  public static GmlType fromString(String type) {
    for (GmlType v : values()) {
      if (v.toString().equals(type)) {
        return v;
      }
    }

    return NONE;
  }

  public static boolean contains(String type) {
    for (GmlType v : values()) {
      if (v.toString().equals(type)) {
        return true;
      }
    }
    return false;
  }

  public boolean isValid() {
    return this != NONE;
  }
}
