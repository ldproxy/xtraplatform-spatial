/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.geoparquet.app;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.sql.domain.SchemaSql.PropertyTypeInfo;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import java.time.ZoneId;
import java.util.Optional;
import org.threeten.extra.Interval;

@SuppressWarnings("PMD.TooManyMethods")
public class SqlDialectDuckdb implements SqlDialect {

  @Override
  public String applyToWkt(String column, boolean forcePolygonCCW, boolean linearizeCurves) {
    return "";
  }

  @Override
  public String applyToWkt(String wkt, int srid) {
    return "";
  }

  @Override
  public String applyToWkb(String column, boolean forcePolygonCCW, boolean linearizeCurves) {
    return "";
  }

  @Override
  public String applyToExtent(String column, boolean is3d) {
    return "";
  }

  @Override
  public String applyToString(String string) {
    return "";
  }

  @Override
  public String applyToDate(String column, Optional<String> format) {
    return "";
  }

  @Override
  public String applyToDatetime(String column, Optional<String> format) {
    return "";
  }

  @Override
  public String applyToDateLiteral(String date) {
    return "";
  }

  @Override
  public String applyToDatetimeLiteral(String datetime) {
    return "";
  }

  @Override
  public String applyToInstantMin() {
    return "";
  }

  @Override
  public String applyToInstantMax() {
    return "";
  }

  @Override
  public String applyToDiameter(String geomExpression, boolean is3d) {
    return "";
  }

  @Override
  public String applyToJsonValue(String alias, String column, String path,
      PropertyTypeInfo typeInfo) {
    return "";
  }

  @Override
  public String castToBigInt(int value) {
    return "";
  }

  @Override
  public Optional<BoundingBox> parseExtent(String extent, EpsgCrs crs) {
    return Optional.empty();
  }

  @Override
  public Optional<Interval> parseTemporalExtent(String start, String end, ZoneId timeZone) {
    return Optional.empty();
  }

  @Override
  public String escapeString(String value) {
    return "";
  }
}
