/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.oracle.app;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.cql.domain.SpatialFunction;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData.QueryGeneratorSettings;
import de.ii.xtraplatform.features.sql.domain.SchemaSql.PropertyTypeInfo;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.threeten.extra.Interval;

// TODO. Notes:
// - only simple feature geometries are supported
// - only 2D geometries are supported / have been tested
// - S_CROSSES not supported
// - identifiers must be unquoted, that is, in uppercase in Oracle
public class SqlDialectOras implements SqlDialect {

  private static final Splitter BBOX_SPLITTER =
      Splitter.onPattern("[(), ]").omitEmptyStrings().trimResults();

  private QueryGeneratorSettings settings;

  /*
    private String applyToGeometry(String column, boolean forcePolygonCCW, boolean linearizeCurves) {
      if (settings.getGeometryAsWkb()) {
        return applyToWkb(column, forcePolygonCCW, linearizeCurves);
      } else {
        return applyToWkt(column, forcePolygonCCW, linearizeCurves);
      }
    }

    private String applyToGeometry(Object geometry, int srid) {
      if (settings.getGeometryAsWkb()) {
        if (geometry instanceof byte[]) {
          return applyToWkb((byte[]) geometry, srid);
        } else if (geometry instanceof String) {
          byte[] wkb = ((String) geometry).getBytes(); // Convert the geometry string to a byte array
          return applyToWkb(wkb, srid);
        } else {
          throw new IllegalArgumentException("Expected geometry to be either byte[] or String.");
        }
      } else {
        if (geometry instanceof String) {
          return applyToWkt((String) geometry, srid);
        } else {
          throw new IllegalArgumentException("Expected geometry to be a String for WKT format.");
        }
      }
    }
  */
  @Override
  public String applyToWkt(String column, boolean forcePolygonCCW, boolean linearizeCurves) {
    if (!forcePolygonCCW) {
      return String.format("SDO_UTIL.TO_WKTGEOMETRY(%s)", column);
    }
    return String.format("SDO_UTIL.TO_WKTGEOMETRY(SDO_UTIL.RECTIFY_GEOMETRY(%s, 0.001))", column);
  }

  @Override
  public String applyToWkt(String wkt, int srid) {
    return String.format("SDO_GEOMETRY('%s',%s)", wkt, srid);
  }

  @Override
  public String applyToWkb(String column, boolean forcePolygonCCW, boolean linearizeCurves) {
    if (!forcePolygonCCW) {
      return String.format("SDO_UTIL.TO_WKBGEOMETRY(%s)", column);
    }
    return String.format("SDO_UTIL.TO_WKBGEOMETRY(SDO_UTIL.RECTIFY_GEOMETRY(%s, 0.001))", column);
  }

  @Override
  public String applyToWkb(byte[] wkb, int srid) {
    return "SDO_UTIL.FROM_WKBGEOMETRY(?, ?)";
  }

  @Override
  public String applyToExtent(String column, boolean is3d) {
    // SDO_AGGR_MBR() results in a 2D Polygon in Oracle Spatial
    return String.format("SDO_UTIL.TO_WKTGEOMETRY(SDO_AGGR_MBR(%s))", column);
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
  public Optional<Interval> parseTemporalExtent(String start, String end) {
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
    return String.format("to_char(%s)", string);
  }

  @Override
  public String applyToDate(String column, Optional<String> format) {
    String date = format.map(f -> String.format("to_date(%s,'%s')", column, f)).orElse(column);
    return String.format("to_char(%s,'YYYY-MM-DD')", date);
  }

  @Override
  public String applyToDatetime(String column, Optional<String> format) {
    String datetime =
        format.map(f -> String.format("to_timestamp_tz(%s,'%s')", column, f)).orElse(column);
    return String.format("to_char(%s,'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')", datetime);
  }

  @Override
  public String applyToDateLiteral(String date) {
    return String.format("to_char(DATE '%s','YYYY-MM-DD')", date);
  }

  @Override
  public String applyToDatetimeLiteral(String datetime) {
    return String.format(
        "to_char(to_utc_timestamp_tz('%s'),'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')", datetime);
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
        "DIAMETER2D()/DIAMETER3D() is not supported for Oracle feature providers.");
  }

  @Override
  public String applyToJsonValue(
      String alias, String column, String path, PropertyTypeInfo typeInfo) {
    throw new IllegalArgumentException("JSON is not supported for Oracle feature providers.");
  }

  @Override
  public String applyToAsIds() {
    return " IDS";
  }

  @Override
  public String applyToLimitAndOffset(long limit, long offset) {
    if (limit > 0 && offset > 0) {
      return String.format(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit);
    } else if (limit > 0) {
      return applyToLimit(limit);
    } else if (offset > 0) {
      return applyToOffset(offset);
    }

    return "";
  }

  @Override
  public String applyToLimit(long limit) {
    return String.format(" FETCH NEXT %d ROWS ONLY", limit);
  }

  @Override
  public String applyToOffset(long offset) {
    return String.format(" OFFSET %d ROWS", offset);
  }

  @Override
  public String applyToNoTable(String select) {
    return String.format("%s FROM dual", select);
  }

  @Override
  public String castToBigInt(int value) {
    return String.format("%d", value);
  }

  @Override
  public String escapeString(String value) {
    return value.replaceAll("'", "''");
  }

  @Override
  public Tuple<String, Optional<String>> getSpatialOperator(
      SpatialFunction spatialFunction, boolean is3d) {
    if (is3d) {
      throw new IllegalArgumentException(
          "3D spatial operators are not supported for Oracle feature providers.");
    }
    Tuple<String, Optional<String>> op = SPATIAL_OPERATORS.get(spatialFunction);
    if (Objects.isNull(op)) {
      throw new IllegalArgumentException(
          String.format(
              "Spatial operator '%s' is not supported for Oracle feature providers.",
              spatialFunction.toString()));
    }
    return op;
  }

  private final Map<SpatialFunction, Tuple<String, Optional<String>>> SPATIAL_OPERATORS =
      new ImmutableMap.Builder<SpatialFunction, Tuple<String, Optional<String>>>()
          .put(SpatialFunction.S_EQUALS, Tuple.of("SDO_EQUAL", Optional.empty()))
          .put(SpatialFunction.S_DISJOINT, Tuple.of("SDO_RELATE", Optional.of("DISJOINT")))
          .put(SpatialFunction.S_TOUCHES, Tuple.of("SDO_TOUCH", Optional.empty()))
          .put(SpatialFunction.S_WITHIN, Tuple.of("SDO_INSIDE", Optional.empty()))
          .put(SpatialFunction.S_OVERLAPS, Tuple.of("SDO_OVERLAPS", Optional.empty()))
          // S_CROSSES is not supported
          .put(SpatialFunction.S_INTERSECTS, Tuple.of("SDO_ANYINTERACT", Optional.empty()))
          .put(SpatialFunction.S_CONTAINS, Tuple.of("SDO_CONTAINS", Optional.empty()))
          .build();

  @Override
  public String getSpatialOperatorMatch(SpatialFunction spatialFunction) {
    return " = 'TRUE'";
  }
}
