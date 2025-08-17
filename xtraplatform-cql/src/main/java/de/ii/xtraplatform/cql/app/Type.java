/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

public enum Type {
  String,
  Boolean,
  Integer,
  Long,
  Double,
  LocalDate,
  Instant,
  Interval,
  OPEN,
  Geometry,
  Bbox,
  List,
  UNKNOWN;

  public String schemaType() {
    return switch (this) {
      case String -> "STRING";
      case Boolean -> "BOOLEAN";
      case Integer, Long -> "INTEGER";
      case Double -> "FLOAT";
      case LocalDate -> "DATE";
      case OPEN -> "UNBOUNDED_START_OR_END";
      case Instant -> "DATETIME";
      case Interval -> "INTERVAL";
      case Geometry -> "GEOMETRY";
      case List -> "ARRAY";
      default -> "unknown";
    };
  }
}
