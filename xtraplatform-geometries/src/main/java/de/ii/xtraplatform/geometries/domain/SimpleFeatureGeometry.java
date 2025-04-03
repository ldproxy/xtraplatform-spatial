/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

/**
 * @author zahnen
 */

/**
 * @langEn The specific geometry type for properties with `type: GEOMETRY`. Possible values are
 *     simple feature geometry types: `POINT`, `MULTI_POINT`, `LINE_STRING`, `MULTI_LINE_STRING`,
 *     `POLYGON`, `MULTI_POLYGON`, `GEOMETRY_COLLECTION` and `ANY`
 * @langDe Mit der Angabe kann der Geometrietype spezifiziert werden. Die Angabe ist nur bei
 *     Geometrieeigenschaften (`type: GEOMETRY`) relevant. Erlaubt sind die
 *     Simple-Feature-Geometrietypen, d.h. `POINT`, `MULTI_POINT`, `LINE_STRING`,
 *     `MULTI_LINE_STRING`, `POLYGON`, `MULTI_POLYGON`, `GEOMETRY_COLLECTION` und `ANY`.
 * @default
 */
public enum SimpleFeatureGeometry {
  POINT,
  MULTI_POINT,
  LINE_STRING,
  MULTI_LINE_STRING,
  POLYGON,
  MULTI_POLYGON,
  GEOMETRY_COLLECTION,
  ANY,
  NONE,
  POLYGON_Z,
  MULTI_POLYGON_Z;

  public boolean isValid() {
    return this != NONE;
  }

  public boolean isSpecific() {
    return isValid() && this != ANY && this != GEOMETRY_COLLECTION;
  }
}
