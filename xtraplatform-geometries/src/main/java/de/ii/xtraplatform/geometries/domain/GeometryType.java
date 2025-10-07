/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * @langEn The specific geometry type for properties with `type: GEOMETRY`. Possible values are
 *     simple feature geometry types: `POINT`, `MULTI_POINT`, `LINE_STRING`, `MULTI_LINE_STRING`,
 *     `POLYGON`, `MULTI_POLYGON`, `GEOMETRY_COLLECTION` and `ANY`. In addition, for feature formats
 *     that support more complex geometries, the types `ANY_EXTENDED`, `CIRCULAR_STRING`,
 *     `COMPOUND_CURVE`, `CURVE_POLYGON`, `MULTI_CURVE`, `MULTI_SURFACE` and `POLYHEDRAL_SURFACE`
 *     are available.
 * @langDe Mit der Angabe kann der Geometrietype spezifiziert werden. Die Angabe ist nur bei
 *     Geometrieeigenschaften (`type: GEOMETRY`) relevant. Erlaubt sind die
 *     Simple-Feature-Geometrietypen, d.h. `POINT`, `MULTI_POINT`, `LINE_STRING`,
 *     `MULTI_LINE_STRING`, `POLYGON`, `MULTI_POLYGON`, `GEOMETRY_COLLECTION` und `ANY`. Zusätzlich
 *     können für Formate, die komplexere Geometrien unterstützen auch die Typen `ANY_EXTENDED`,
 *     `CIRCULAR_STRING`, `COMPOUND_CURVE`, `CURVE_POLYGON`, `MULTI_CURVE`, `MULTI_SURFACE` und
 *     `POLYHEDRAL_SURFACE` verwendet werden.
 * @default null
 */
public enum GeometryType {
  ANY(true, null, null),
  POINT(true, 0, false),
  LINE_STRING(true, 1, false),
  POLYGON(true, 2, false),
  MULTI_POINT(true, 0, false),
  MULTI_LINE_STRING(true, 1, false),
  MULTI_POLYGON(true, 2, false),
  POLYHEDRAL_SURFACE(true, 2, false),
  GEOMETRY_COLLECTION(true, null, true),
  ANY_EXTENDED(false, null, null),
  CIRCULAR_STRING(false, 1, false),
  COMPOUND_CURVE(false, 1, true),
  CURVE_POLYGON(false, 2, true),
  MULTI_CURVE(false, 1, true),
  MULTI_SURFACE(false, 2, true);

  public static final Set<GeometryType> DEFAULT_GEOMETRY_TYPES =
      Set.of(POINT, MULTI_POINT, LINE_STRING, MULTI_LINE_STRING, POLYGON, MULTI_POLYGON);

  public static boolean onlySimpleFeatureGeometries(Set<GeometryType> geometryTypes) {
    return geometryTypes.stream().allMatch(GeometryType::isSimpleFeature);
  }

  private final boolean isSimpleFeature;
  private final Integer geometryDimension;
  private final Boolean hasNestedGeometries;

  GeometryType(boolean isSimpleFeature, Integer geometryDimension, Boolean hasNestedGeometries) {
    this.isSimpleFeature = isSimpleFeature;
    this.geometryDimension = geometryDimension;
    this.hasNestedGeometries = hasNestedGeometries;
  }

  public Optional<Integer> getGeometryDimension() {
    return Optional.ofNullable(geometryDimension);
  }

  public boolean isSimpleFeature() {
    return isSimpleFeature;
  }

  public boolean hasNestedGeometries() {
    return Objects.requireNonNullElse(hasNestedGeometries, false);
  }

  public boolean isAbstract() {
    return this == ANY || this == ANY_EXTENDED;
  }

  public boolean isSpecific() {
    return !isAbstract();
  }
}
