/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.domain;

import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;

public enum GeoJsonGeometryType {
  POINT("Point", SimpleFeatureGeometry.POINT),
  MULTI_POINT("MultiPoint", SimpleFeatureGeometry.MULTI_POINT),
  LINE_STRING("LineString", SimpleFeatureGeometry.LINE_STRING),
  MULTI_LINE_STRING("MultiLineString", SimpleFeatureGeometry.MULTI_LINE_STRING),
  POLYGON("Polygon", SimpleFeatureGeometry.POLYGON),
  MULTI_POLYGON("MultiPolygon", SimpleFeatureGeometry.MULTI_POLYGON),
  GEOMETRY_COLLECTION("GeometryCollection", SimpleFeatureGeometry.GEOMETRY_COLLECTION),
  NONE("", SimpleFeatureGeometry.NONE);

  private String stringRepresentation;
  private SimpleFeatureGeometry sfType;

  GeoJsonGeometryType(String stringRepresentation, SimpleFeatureGeometry sfType) {
    this.stringRepresentation = stringRepresentation;
    this.sfType = sfType;
  }

  @Override
  public String toString() {
    return stringRepresentation;
  }

  public SimpleFeatureGeometry toSimpleFeatureGeometry() {
    return sfType;
  }

  public static GeoJsonGeometryType forString(String type) {
    for (GeoJsonGeometryType geoJsonType : GeoJsonGeometryType.values()) {
      if (geoJsonType.toString().equals(type)) {
        return geoJsonType;
      }
    }

    return NONE;
  }

  public static GeoJsonGeometryType forSimpleFeatureType(SimpleFeatureGeometry type) {
    for (GeoJsonGeometryType geoJsonType : GeoJsonGeometryType.values()) {
      if (geoJsonType.sfType.equals(type)) {
        return geoJsonType;
      }
    }

    return NONE;
  }

  public boolean isValid() {
    return this != NONE;
  }

  public boolean isSupported() {
    return isValid() && this != GEOMETRY_COLLECTION;
  }
}
