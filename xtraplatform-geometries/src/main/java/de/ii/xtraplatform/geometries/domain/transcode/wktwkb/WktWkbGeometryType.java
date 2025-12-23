/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transcode.wktwkb;

import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.GeometryType;

public enum WktWkbGeometryType {
  POINT,
  MULTIPOINT,
  LINESTRING,
  MULTILINESTRING,
  POLYGON,
  MULTIPOLYGON,
  GEOMETRYCOLLECTION,
  CIRCULARSTRING,
  COMPOUNDCURVE,
  CURVEPOLYGON,
  MULTICURVE,
  MULTISURFACE,
  POLYHEDRALSURFACE,
  GEOMETRY,
  NONE;

  public static WktWkbGeometryType fromWkbType(int type) {
    if (type > Axes.SPECIAL_SHIFT) {
      // Special encoding used by Oracle; For example, 1000003 is a CURVEPOLYGON
      return switch (type % Axes.SPECIAL_SHIFT) {
        case 1 -> CIRCULARSTRING;
        case 2 -> COMPOUNDCURVE;
        case 3 -> CURVEPOLYGON;
        case 4 -> MULTICURVE;
        case 5 -> MULTISURFACE;
        default -> NONE;
      };
    }
    return switch (type % 1000) {
      case 0 -> GEOMETRY;
      case 1 -> POINT;
      case 2 -> LINESTRING;
      case 3 -> POLYGON;
      case 4 -> MULTIPOINT;
      case 5 -> MULTILINESTRING;
      case 6 -> MULTIPOLYGON;
      case 7 -> GEOMETRYCOLLECTION;
      case 8 -> CIRCULARSTRING;
      case 9 -> COMPOUNDCURVE;
      case 10 -> CURVEPOLYGON;
      case 11 -> MULTICURVE;
      case 12 -> MULTISURFACE;
      case 15 -> POLYHEDRALSURFACE;
      default -> NONE;
    };
  }

  public static WktWkbGeometryType fromGeometryType(GeometryType type) {
    return switch (type) {
      case POINT -> POINT;
      case MULTI_POINT -> MULTIPOINT;
      case LINE_STRING -> LINESTRING;
      case MULTI_LINE_STRING -> MULTILINESTRING;
      case POLYGON -> POLYGON;
      case MULTI_POLYGON -> MULTIPOLYGON;
      case GEOMETRY_COLLECTION -> GEOMETRYCOLLECTION;
      case CIRCULAR_STRING -> CIRCULARSTRING;
      case COMPOUND_CURVE -> COMPOUNDCURVE;
      case CURVE_POLYGON -> CURVEPOLYGON;
      case MULTI_CURVE -> MULTICURVE;
      case MULTI_SURFACE -> MULTISURFACE;
      case POLYHEDRAL_SURFACE -> POLYHEDRALSURFACE;
      case ANY -> GEOMETRY;
      default -> NONE;
    };
  }

  public int toWkbCode(Axes axes) {
    int baseCode =
        switch (this) {
          case GEOMETRY -> 0;
          case POINT -> 1;
          case LINESTRING -> 2;
          case POLYGON -> 3;
          case MULTIPOINT -> 4;
          case MULTILINESTRING -> 5;
          case MULTIPOLYGON -> 6;
          case GEOMETRYCOLLECTION -> 7;
          case CIRCULARSTRING -> 8;
          case COMPOUNDCURVE -> 9;
          case CURVEPOLYGON -> 10;
          case MULTICURVE -> 11;
          case MULTISURFACE -> 12;
          case POLYHEDRALSURFACE -> 15;
          default -> throw new IllegalStateException("Unsupported geometry type: " + this);
        };

    return switch (axes) {
      case XY -> baseCode;
      case XYZ -> baseCode + 1000;
      case XYM -> baseCode + 2000;
      case XYZM -> baseCode + 3000;
    };
  }

  public GeometryType toGeometryType() {
    return switch (this) {
      case POINT -> GeometryType.POINT;
      case MULTIPOINT -> GeometryType.MULTI_POINT;
      case LINESTRING -> GeometryType.LINE_STRING;
      case MULTILINESTRING -> GeometryType.MULTI_LINE_STRING;
      case POLYGON -> GeometryType.POLYGON;
      case MULTIPOLYGON -> GeometryType.MULTI_POLYGON;
      case GEOMETRYCOLLECTION -> GeometryType.GEOMETRY_COLLECTION;
      case CIRCULARSTRING -> GeometryType.CIRCULAR_STRING;
      case COMPOUNDCURVE -> GeometryType.COMPOUND_CURVE;
      case CURVEPOLYGON -> GeometryType.CURVE_POLYGON;
      case MULTICURVE -> GeometryType.MULTI_CURVE;
      case MULTISURFACE -> GeometryType.MULTI_SURFACE;
      case POLYHEDRALSURFACE -> GeometryType.POLYHEDRAL_SURFACE;
      default -> GeometryType.ANY;
    };
  }

  public boolean isValid() {
    return this != NONE;
  }

  public boolean isAbstract() {
    return this == GEOMETRY;
  }
}
