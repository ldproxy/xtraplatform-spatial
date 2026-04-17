/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.geoparquet.app;

import com.google.common.base.Splitter;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.sql.domain.SchemaSql.PropertyTypeInfo;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.threeten.extra.Interval;

/** This implementation of the DuckDB SQL-dialect is for the purpose of reading GeoParquet-files. */
@SuppressWarnings("PMD.TooManyMethods")
public class SqlDialectDuckdb implements SqlDialect {

  private static final Splitter BBOX_SPLITTER =
      Splitter.onPattern("[(), ]").omitEmptyStrings().trimResults();

  /**
   * As of v.1.1.0, the GeoParquet standard curve geometries are not supported and all exterior
   * polygon rings must already be ordered in CCW directorion, see "geometry_types" under <a
   * href="https://geoparquet.org/releases/v1.1.0/">https://geoparquet.org/releases/v1.1.0/</a>
   */
  @Override
  public String applyToWkt(String column, boolean forcePolygonCCW, boolean linearizeCurves) {
    return String.format("ST_AsText(%s)", column);
  }

  /**
   * As of v.1.1.0, geometries are encoded using WKB or the single-geometry type encodings based on
   * the GeoArrow specification, see <a
   * href="https://geoparquet.org/releases/v1.1.0/">https://geoparquet.org/releases/v1.1.0/</a>.
   * Therefore this function should not be necessary when working with GeoParquet files.
   */
  @Override
  public String applyToWkt(String wkt, int srid) {
    // return String.format("ST_TRANSFORM(ST_GeomFromText('%s'), 'EPSG:%s')", wkt, srid);
    return String.format("ST_GeomFromText('%s')", wkt);
  }

  /**
   * As of v.1.1.0, the GeoParquet standard curve geometries are not supported and all exterior
   * polygon rings must already be ordered in CCW directorion, see "geometry_types" under <a
   * href="https://geoparquet.org/releases/v1.1.0/">https://geoparquet.org/releases/v1.1.0/</a>
   */
  @Override
  public String applyToWkb(String column, boolean forcePolygonCCW, boolean linearizeCurves) {
    return String.format("ST_AsWKB(%s)", column);
  }

  @Override
  public String applyToExtent(String column, boolean is3d) {
    if (is3d)
      throw new IllegalArgumentException("3d is not supported for GeoParquet feature providers.");

    return String.format("ST_AsText(ST_Extent(%s))", column);
  }

  @Override
  public Optional<BoundingBox> parseExtent(String extent, EpsgCrs crs) {
    if (Objects.isNull(extent)) {
      return Optional.empty();
    }

    // Example for what DuckDB returns: BOX(7.1053586 50.6424865, 7.2106794 50.7177159)
    if (extent.contains("BOX")) {
      extent = extent.replaceFirst("BOX", "");
    }
    List<String> bbox = BBOX_SPLITTER.splitToList(extent);

    if (bbox.size() == 4) {
      return Optional.of(
          BoundingBox.of(
              Double.parseDouble(bbox.get(0)),
              Double.parseDouble(bbox.get(1)),
              Double.parseDouble(bbox.get(2)),
              Double.parseDouble(bbox.get(3)),
              crs));
    }

    return Optional.empty();
  }

  @Override
  public Optional<Interval> parseTemporalExtent(String start, String end, ZoneId timeZone) {
    if (Objects.isNull(start)) {
      return Optional.empty();
    }

    DateTimeFormatter parser;
    if (Objects.isNull(timeZone)) {
      parser =
          DateTimeFormatter.ofPattern("yyyy-MM-dd[['T'][' ']HH:mm:ss][.SSS][X]")
              .withZone(ZoneOffset.UTC);
    } else {
      parser =
          DateTimeFormatter.ofPattern("yyyy-MM-dd[['T'][' ']HH:mm:ss][.SSS][X]").withZone(timeZone);
    }

    // Necessary because DUCKDB supports INFINITY
    // Note: Strictly, INFINITY/-INFINITY and INSTANT.MAX/INSTANT.MIN are not the same! May result
    // in undesired behavior.
    Instant parsedStart;
    switch (start.toUpperCase()) {
      case "-INFINITY" -> parsedStart = Instant.MIN;
      case "INFINITY" -> parsedStart = Instant.MAX;
      default -> parsedStart = parser.parse(start, Instant::from);
    }

    if (Objects.isNull(end)) {
      return Optional.of(Interval.of(parsedStart, Instant.MAX));
    }

    Instant parsedEnd;
    switch (end.toUpperCase()) {
      case "INFINITY" -> parsedEnd = Instant.MAX;
      case "-INFINITY" -> parsedEnd = Instant.MIN;
      default -> parsedEnd = parser.parse(end, Instant::from);
    }

    return Optional.of(Interval.of(parsedStart, parsedEnd));
  }

  @Override
  public String applyToString(String string) {
    return String.format("CAST(%s AS VARCHAR)", string);
  }

  @Override
  public String applyToDate(String column, Optional<String> format) {
    if (format.isEmpty()) return String.format("CAST(%s AS DATE)", column);

    return String.format("strftime(CAST(%s AS DATE), '%s')", column, format);
  }

  @Override
  public String applyToDatetime(String column, Optional<String> format) {
    if (format.isEmpty()) return String.format("CAST(%s AS TIMESTAMP)", column);

    return String.format("strftime(CAST(%s AS TIMESTAMP), '%s')", column, format);
  }

  @Override
  public String applyToDateLiteral(String date) {
    return String.format("CAST('%s' AS DATE)", date);
  }

  @Override
  public String applyToDatetimeLiteral(String datetime) {
    return String.format("CAST('%s' AS TIMESTAMP)", datetime);
  }

  @Override
  public String applyToInstantMin() {
    return "-INFINITY";
  }

  @Override
  public String applyToInstantMax() {
    return "INFINITY";
  }

  @Override
  public String applyToDiameter(String geomExpression, boolean is3d) {
    throw new IllegalArgumentException(
        "DIAMETER2D()/DIAMETER3D() is not supported for GeoParquet feature providers.");
  }

  @Override
  public String applyToJsonValue(
      String alias, String column, String path, PropertyTypeInfo typeInfo) {
    // ToDo: Implement
    throw new IllegalArgumentException(
        "JSON is not supported for GeoParquet feature providers yet.");
  }

  @Override
  public String castToBigInt(int value) {
    return String.format("CAST(%d AS BIGINT)", value);
  }

  @Override
  public String escapeString(String value) {
    return value.replace("'", "''");
  }
}
