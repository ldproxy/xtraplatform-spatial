/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;

/** Handles conversion between SimpleFeatureGeometry and WKB types. */
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

  public static SimpleFeatureGeometryFromToWkb fromWkbType(int type) {
    boolean hasZ = (type & 0x80000000) != 0; // Prüfen, ob das WKB25D-Flag gesetzt ist
    type &= ~0x80000000; // Entfernen des WKB25D-Flags, um den Basistyp zu bestimmen

    switch (type) {
      case 1:
        return POINT;
      case 2:
        return LINESTRING;
      case 3:
        return POLYGON;
      case 4:
        return MULTIPOINT;
      case 5:
        return MULTILINESTRING;
      case 6:
        return MULTIPOLYGON;
      case 7:
        return GEOMETRYCOLLECTION;
      default:
        return NONE;
    }
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
      case ANY:
        return SimpleFeatureGeometry.ANY;
      default:
        return SimpleFeatureGeometry.NONE;
    }
  }

  public boolean isValid() {
    return this != NONE;
  }
}
