/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;
import org.locationtech.jts.geom.Geometry;

public enum SimpleFeatureGeometryFromToWkb {
  POINT,
  MULTIPOINT,
  LINESTRING,
  MULTILINESTRING,
  POLYGON,
  MULTIPOLYGON,
  GEOMETRYCOLLECTION,
  ANY,
  NONE;

  public static SimpleFeatureGeometry fromGeometry(Geometry geometry) {
    // Implement the logic to convert a Geometry to SimpleFeatureGeometry
    switch (geometry.getGeometryType().toUpperCase()) {
      case "POINT":
        return SimpleFeatureGeometry.POINT;
      case "MULTIPOINT":
        return SimpleFeatureGeometry.MULTI_POINT;
      case "LINESTRING":
        return SimpleFeatureGeometry.LINE_STRING;
      case "MULTILINESTRING":
        return SimpleFeatureGeometry.MULTI_LINE_STRING;
      case "POLYGON":
        return SimpleFeatureGeometry.POLYGON;
      case "MULTIPOLYGON":
        return SimpleFeatureGeometry.MULTI_POLYGON;
      case "GEOMETRYCOLLECTION":
        return SimpleFeatureGeometry.GEOMETRY_COLLECTION;
      default:
        return SimpleFeatureGeometry.NONE;
    }
  }
}
