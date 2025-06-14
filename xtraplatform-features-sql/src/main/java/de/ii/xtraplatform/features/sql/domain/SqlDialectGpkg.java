/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.google.common.base.Splitter;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData.QueryGeneratorSettings;
import de.ii.xtraplatform.features.sql.domain.SchemaSql.PropertyTypeInfo;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.threeten.extra.Interval;

public class SqlDialectGpkg implements SqlDialect {

  private QueryGeneratorSettings settings;

  private static final Splitter BBOX_SPLITTER =
      Splitter.onPattern("[(), ]").omitEmptyStrings().trimResults();

  @Override
  public String applyToWkt(String column, boolean forcePolygonCCW, boolean linearizeCurves) {
    if (!forcePolygonCCW) {
      return String.format("ST_AsText(%s)", column);
    }
    return String.format("ST_AsText(ST_ForcePolygonCCW(%s))", column);
  }

  @Override
  public String applyToWkt(String wkt, int srid) {
    return String.format("ST_GeomFromText('%s',%s)", wkt, srid);
  }

  @Override
  public String applyToWkb(String column, boolean forcePolygonCCW, boolean linearizeCurves) {
    if (!forcePolygonCCW) {
      return String.format("ST_AsBinary(%s)", column);
    }
    return String.format("ST_AsBinary(ST_ForcePolygonCCW(%s))", column);
  }

  @Override
  public String applyToExtent(String column, boolean is3d) {
    // Extent() results in a 2D Polygon in Spatialite
    return String.format("ST_AsText(Extent(%s))", column);
  }

  @Override
  public Optional<BoundingBox> parseExtent(String extent, EpsgCrs crs) {
    if (Objects.isNull(extent)) {
      return Optional.empty();
    }

    List<String> bbox = BBOX_SPLITTER.splitToList(extent);

    if (bbox.size() > 6) {
      return Optional.of(
          BoundingBox.of(
              Double.parseDouble(bbox.get(1)),
              Double.parseDouble(bbox.get(2)),
              Double.parseDouble(bbox.get(5)),
              Double.parseDouble(bbox.get(6)),
              crs));
    }

    return Optional.empty();
  }

  @Override
  public Optional<Interval> parseTemporalExtent(String start, String end, ZoneId timeZone) {
    if (Objects.isNull(start)) {
      return Optional.empty();
    }
    DateTimeFormatter parser =
        DateTimeFormatter.ofPattern("yyyy-MM-dd[['T'][' ']HH:mm:ss][.SSS][X]")
            .withZone(ZoneOffset.UTC);
    Instant parsedStart = parser.parse(start, Instant::from);
    if (Objects.isNull(end)) {
      return Optional.of(Interval.of(parsedStart, Instant.MAX));
    }
    Instant parsedEnd = parser.parse(end, Instant::from);
    return Optional.of(Interval.of(parsedStart, parsedEnd));
  }

  @Override
  public String applyToString(String string) {
    return String.format("cast(%s as text)", string);
  }

  @Override
  public String applyToDate(String column, Optional<String> format) {
    return String.format("date(%s)", column);
  }

  @Override
  public String applyToDatetime(String column, Optional<String> format) {
    return String.format("datetime(%s)", column);
  }

  @Override
  public String applyToDateLiteral(String date) {
    return String.format("date('%s')", date);
  }

  @Override
  public String applyToDatetimeLiteral(String datetime) {
    return String.format("datetime('%s')", datetime);
  }

  @Override
  public String applyToInstantMin() {
    return "0001-01-01T00:00:00Z";
  }
  ;

  @Override
  public String applyToInstantMax() {
    return "9999-12-31T23:59:59Z";
  }

  @Override
  public String applyToDiameter(String geomExpression, boolean is3d) {
    throw new IllegalArgumentException(
        "DIAMETER2D()/DIAMETER3D() is not supported for GeoPackage feature providers.");
  }

  @Override
  public String applyToJsonValue(
      String alias, String column, String path, PropertyTypeInfo typeInfo) {

    if (typeInfo.getInArray()) {
      throw new IllegalArgumentException(
          "Queryables that are values in an array are not supported for GeoPackage feature providers.");
    }

    String cast = "";
    if (Objects.nonNull(typeInfo.getType())) {
      switch (typeInfo.getType()) {
        case STRING:
        case FLOAT:
        case INTEGER:
        case BOOLEAN:
          cast = getCast(typeInfo.getType());
          break;
        case VALUE:
        case FEATURE_REF:
          cast = typeInfo.getValueType().map(this::getCast).orElse(getCast(Type.STRING));
          break;
        case VALUE_ARRAY:
        case FEATURE_REF_ARRAY:
          throw new IllegalArgumentException(
              "Arrays as queryables are not supported for GeoPackage feature providers.");
      }
    }

    if (Objects.isNull(path)) {
      if (cast.isEmpty()) {
        return String.format("%s.%s", alias, column);
      }
      return String.format("cast(%s.%s as %s)", alias, column, cast);
    }
    if (cast.isEmpty()) {
      return String.format("%s.%s ->> '$.%s'", alias, column, path);
    }
    return String.format("cast((%s.%s ->> '$.%s') as %s)", alias, column, path, cast);
  }

  private String getCast(Type valueType) {
    switch (valueType) {
      case FLOAT:
        return "real";
      case INTEGER:
      case BOOLEAN:
        return "integer";
      default:
      case STRING:
        return "text";
    }
  }

  @Override
  public String castToBigInt(int value) {
    return String.format("CAST(%d AS BIGINT)", value);
  }

  @Override
  public String escapeString(String value) {
    return value.replaceAll("'", "''");
  }

  @Override
  public String applyToExpression(
      String table, String name, Map<String, String> subDecoderPaths, boolean spatial) {
    if (!subDecoderPaths.isEmpty()) {
      String expression =
          subDecoderPaths.values().iterator().next().replaceAll("\\$(?:t|T|table)\\$", table);
      if (spatial && settings.getGeometryAsWkb()) {
        expression = applyToWkb(expression, false, false);
      } else if (spatial) {
        expression = applyToWkt(expression, false, false);
      }
      return String.format("(%s) AS %s", expression, name);
    }
    return SqlDialect.super.applyToExpression(table, name, subDecoderPaths, spatial);
  }
}
