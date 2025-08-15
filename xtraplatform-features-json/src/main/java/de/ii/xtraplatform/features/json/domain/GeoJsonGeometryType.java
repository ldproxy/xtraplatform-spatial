/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.domain;

import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryType;

public enum GeoJsonGeometryType {
  POINT("Point", GeometryType.POINT),
  MULTI_POINT("MultiPoint", GeometryType.MULTI_POINT),
  LINE_STRING("LineString", GeometryType.LINE_STRING),
  MULTI_LINE_STRING("MultiLineString", GeometryType.MULTI_LINE_STRING),
  POLYGON("Polygon", GeometryType.POLYGON),
  MULTI_POLYGON("MultiPolygon", GeometryType.MULTI_POLYGON),
  MULTI_POLYGON2("MultiPolygon", GeometryType.POLYHEDRAL_SURFACE),
  GEOMETRY_COLLECTION("GeometryCollection", GeometryType.GEOMETRY_COLLECTION),
  NONE("", GeometryType.ANY);

  private final String stringRepresentation;
  private final GeometryType geometryType;

  GeoJsonGeometryType(String stringRepresentation, GeometryType geometryType) {
    this.stringRepresentation = stringRepresentation;
    this.geometryType = geometryType;
  }

  @Override
  public String toString() {
    return stringRepresentation;
  }

  public GeometryType toGeometryType() {
    return geometryType;
  }

  public static GeoJsonGeometryType forString(String type) {
    for (GeoJsonGeometryType geoJsonType : GeoJsonGeometryType.values()) {
      if (geoJsonType.toString().equals(type)) {
        return geoJsonType;
      }
    }

    return NONE;
  }

  public static GeoJsonGeometryType forGeometry(Geometry<?> geometry) {
    for (GeoJsonGeometryType geoJsonType : GeoJsonGeometryType.values()) {
      if (geoJsonType.geometryType != null && geoJsonType.geometryType.equals(geometry.getType())) {
        return geoJsonType;
      }
    }

    return NONE;
  }

  public boolean isValid() {
    return this != NONE;
  }

  public boolean isSupported() {
    return isValid();
  }
}
