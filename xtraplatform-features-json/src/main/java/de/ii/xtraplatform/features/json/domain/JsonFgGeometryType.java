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
import de.ii.xtraplatform.geometries.domain.PolyhedralSurface;
import java.util.Optional;

// TODO add unit tests for all geometry types
public enum JsonFgGeometryType {
  POINT("Point", GeometryType.POINT),
  MULTI_POINT("MultiPoint", GeometryType.MULTI_POINT),
  LINE_STRING("LineString", GeometryType.LINE_STRING),
  MULTI_LINE_STRING("MultiLineString", GeometryType.MULTI_LINE_STRING),
  POLYGON("Polygon", GeometryType.POLYGON),
  MULTI_POLYGON("MultiPolygon", GeometryType.MULTI_POLYGON),
  MULTI_POLYGON2("Polyhedron", GeometryType.POLYHEDRAL_SURFACE),
  GEOMETRY_COLLECTION("GeometryCollection", GeometryType.GEOMETRY_COLLECTION),
  CIRCULAR_STRING("CircularString", GeometryType.CIRCULAR_STRING),
  COMPOUND_CURVE("CompoundCurve", GeometryType.COMPOUND_CURVE),
  CURVE_POLYGON("CurvePolygon", GeometryType.CURVE_POLYGON),
  MULTI_CURVE("MultiCurve", GeometryType.MULTI_CURVE),
  MULTI_SURFACE("MultiSurface", GeometryType.MULTI_SURFACE),
  POLYHEDRON("Polyhedron", GeometryType.POLYHEDRAL_SURFACE),
  NONE("", GeometryType.ANY),
  NONE2("", GeometryType.ANY_EXTENDED);

  private final String stringRepresentation;
  private final GeometryType geometryType;

  JsonFgGeometryType(String stringRepresentation, GeometryType geometryType) {
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

  public static JsonFgGeometryType forString(String type) {
    for (JsonFgGeometryType jsonFgType : JsonFgGeometryType.values()) {
      if (jsonFgType.toString().equals(type)) {
        return jsonFgType;
      }
    }

    return NONE;
  }

  public static JsonFgGeometryType forGeometry(Geometry<?> geometry) {
    // Special handling of polyhedra
    if (geometry.getType() == GeometryType.POLYHEDRAL_SURFACE
        && ((PolyhedralSurface) geometry).isClosed()) {
      return POLYHEDRON;
    }

    for (JsonFgGeometryType jsonFgType : JsonFgGeometryType.values()) {
      if (jsonFgType.geometryType.equals(geometry.getType())) {
        return jsonFgType;
      }
    }

    return NONE;
  }

  public static Optional<Integer> getGeometryDimension(
      GeometryType type, boolean isComposite, boolean isClosed) {
    if (type == GeometryType.POLYHEDRAL_SURFACE && isClosed) {
      return Optional.of(3);
    } else if (type == GeometryType.MULTI_POLYGON && isClosed && isComposite) {
      return Optional.of(3);
    }
    return type.getGeometryDimension();
  }

  public boolean isValid() {
    return this != NONE && this != NONE2;
  }

  public boolean isSupported() {
    return isValid();
  }
}
