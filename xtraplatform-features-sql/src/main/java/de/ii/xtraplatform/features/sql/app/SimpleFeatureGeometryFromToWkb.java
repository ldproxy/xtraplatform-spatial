/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;

public enum SimpleFeatureGeometryFromToWkb {
  POINT,
  MULTIPOINT,
  LINESTRING,
  MULTILINESTRING,
  POLYGON,
  MULTIPOLYGON,
  GEOMETRYCOLLECTION,
  NONE;

  public static SimpleFeatureGeometryFromToWkb fromWkbType(int type) {
    SimpleFeatureGeometryFromToWkb result;
    switch (type) {
      case 1:
        result = POINT;
        break;
      case 2:
        result = LINESTRING;
        break;
      case 3:
        result = POLYGON;
        break;
      case 4:
        result = MULTIPOINT;
        break;
      case 5:
        result = MULTILINESTRING;
        break;
      case 6:
      case 1006:
        result = MULTIPOLYGON;
        break;
      case 1005:
        result = MULTILINESTRING;
        break;
      case 1002:
        result = LINESTRING;
        break;
      case 1003:
        result = POLYGON;
        break;
      case 1004:
        result = MULTIPOINT;
        break;
      case 1001:
        result = POINT;
        break;
      case 7:
        result = GEOMETRYCOLLECTION;
        break;
      default:
        result = NONE;
    }
    return result;
  }

  public SimpleFeatureGeometry toSimpleFeatureGeometry() {
    switch (this) {
      case POINT:
        return SimpleFeatureGeometry.POINT;
      case MULTIPOINT:
        return SimpleFeatureGeometry.MULTI_POINT;
      case LINESTRING:
        return SimpleFeatureGeometry.LINE_STRING;
      case MULTILINESTRING:
        return SimpleFeatureGeometry.MULTI_LINE_STRING;
      case POLYGON:
        return SimpleFeatureGeometry.POLYGON;
      case MULTIPOLYGON:
        return SimpleFeatureGeometry.MULTI_POLYGON;
      case GEOMETRYCOLLECTION:
        return SimpleFeatureGeometry.GEOMETRY_COLLECTION;
      default:
        return SimpleFeatureGeometry.NONE;
    }
  }

  public boolean isValid() {
    return this != NONE;
  }
}
