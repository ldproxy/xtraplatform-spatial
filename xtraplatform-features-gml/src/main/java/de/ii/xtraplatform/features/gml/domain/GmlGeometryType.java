/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

import de.ii.xtraplatform.geometries.domain.GeometryType;

public enum GmlGeometryType {
  GEOMETRY("GeometryPropertyType"),
  ABSTRACT_GEOMETRY("GeometricPrimitivePropertyType"),
  POINT("PointPropertyType", "Point"),
  MULTI_POINT("MultiPointPropertyType", "MultiPoint"),
  LINE_STRING("LineStringPropertyType", "LineString"),
  MULTI_LINESTRING("MultiLineStringPropertyType", "MultiLineString"),
  CURVE("CurvePropertyType", "Curve"),
  MULTI_CURVE("MultiCurvePropertyType", "MultiCurve"),
  SURFACE("SurfacePropertyType", "Surface"),
  MULTI_SURFACE("MultiSurfacePropertyType", "MultiSurface"),
  POLYGON("PolygonPropertyType", "Polygon"),
  MULTI_POLYGON("MultiPolygonPropertyType", "MultiPolygon"),
  SOLID("SolidPropertyType"),
  NONE("");

  private String stringRepresentation;
  private String elementStringRepresentation;

  GmlGeometryType(String stringRepresentation) {
    this.stringRepresentation = stringRepresentation;
  }

  GmlGeometryType(String stringRepresentation, String elementStringRepresentation) {
    this(stringRepresentation);
    this.elementStringRepresentation = elementStringRepresentation;
  }

  @Override
  public String toString() {
    return stringRepresentation;
  }

  public static GmlGeometryType fromString(String type) {
    for (GmlGeometryType v : GmlGeometryType.values()) {
      if (v.toString().equals(type)
          || (v.elementStringRepresentation != null
              && v.elementStringRepresentation.equals(type))) {
        return v;
      }
    }
    return NONE;
  }

  public static boolean contains(String type) {
    for (GmlGeometryType v : GmlGeometryType.values()) {
      if (v.toString().equals(type)) {
        return true;
      }
    }
    return false;
  }

  public GeometryType toSimpleFeatureGeometry() {
    return switch (this) {
      case POINT -> GeometryType.POINT;
      case MULTI_POINT -> GeometryType.MULTI_POINT;
      case LINE_STRING, CURVE -> GeometryType.LINE_STRING;
      case MULTI_LINESTRING, MULTI_CURVE -> GeometryType.MULTI_LINE_STRING;
      case SURFACE, POLYGON -> GeometryType.POLYGON;
      case MULTI_SURFACE, MULTI_POLYGON -> GeometryType.MULTI_POLYGON;
      default -> GeometryType.ANY;
    };
  }

  public boolean isValid() {
    return this != NONE;
  }
}
