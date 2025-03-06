/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import de.ii.xtraplatform.cql.domain.ArrayFunction;
import de.ii.xtraplatform.cql.domain.SpatialFunction;
import de.ii.xtraplatform.cql.domain.TemporalFunction;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.domain.SchemaSql.PropertyTypeInfo;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.threeten.extra.Interval;

public interface SqlDialect {

  String applyToWkt(String column, boolean forcePolygonCCW, boolean linearizeCurves);

  String applyToWkt(String wkt, int srid);

  String applyToWkb(String column, boolean forcePolygonCCW, boolean linearizeCurves);

  String applyToWkb(byte[] wkb, int srid);

  String applyToExtent(String column, boolean is3d);

  String applyToString(String string);

  String applyToDate(String column, Optional<String> format);

  String applyToDatetime(String column, Optional<String> format);

  String applyToDateLiteral(String date);

  String applyToDatetimeLiteral(String datetime);

  String applyToInstantMin();

  String applyToInstantMax();

  String applyToDiameter(String geomExpression, boolean is3d);

  String applyToJsonValue(String alias, String column, String path, PropertyTypeInfo typeInfo);

  default String applyToJsonArrayOp(
      ArrayFunction op, boolean notInverse, String mainExpression, String jsonValueArray) {
    throw new IllegalArgumentException(
        "Arrays as queryables are not supported for this feature provider.");
  }

  default String applyToAsIds() {
    return " AS IDS";
  }

  default String applyToLimitAndOffset(long limit, long offset) {
    if (limit > 0 && offset > 0) {
      return String.format(" LIMIT %d OFFSET %d", limit, offset);
    } else if (limit > 0) {
      return applyToLimit(limit);
    } else if (offset > 0) {
      return applyToOffset(offset);
    }

    return "";
  }

  default String applyToLimit(long limit) {
    return String.format(" LIMIT %d", limit);
  }

  default String applyToOffset(long offset) {
    return String.format(" OFFSET %d", offset);
  }

  default String applyToNoTable(String select) {
    return select;
  }

  String castToBigInt(int value);

  Optional<BoundingBox> parseExtent(String extent, EpsgCrs crs);

  Optional<Interval> parseTemporalExtent(String start, String end);

  String escapeString(String value);

  default Tuple<String, Optional<String>> getSpatialOperator(
      SpatialFunction spatialFunction, boolean is3d) {
    return is3d && SPATIAL_OPERATORS_3D.containsKey(spatialFunction)
        ? Tuple.of(SPATIAL_OPERATORS_3D.get(spatialFunction), Optional.empty())
        : Tuple.of(SPATIAL_OPERATORS.get(spatialFunction), Optional.empty());
  }

  default String getSpatialOperatorMatch(SpatialFunction spatialFunction) {
    return "";
  }

  default String getTemporalOperator(TemporalFunction temporalFunction) {
    // this is implementation specific
    return null;
  }

  default Set<TemporalFunction> getTemporalOperators() {
    return ImmutableSet.of();
  }

  default String applyToExpression(
      String table, String name, Map<String, String> subDecoderPaths, boolean spatial) {
    return name;
  }

  Map<SpatialFunction, String> SPATIAL_OPERATORS =
      new ImmutableMap.Builder<SpatialFunction, String>()
          .put(SpatialFunction.S_EQUALS, "ST_Equals")
          .put(SpatialFunction.S_DISJOINT, "ST_Disjoint")
          .put(SpatialFunction.S_TOUCHES, "ST_Touches")
          .put(SpatialFunction.S_WITHIN, "ST_Within")
          .put(SpatialFunction.S_OVERLAPS, "ST_Overlaps")
          .put(SpatialFunction.S_CROSSES, "ST_Crosses")
          .put(SpatialFunction.S_INTERSECTS, "ST_Intersects")
          .put(SpatialFunction.S_CONTAINS, "ST_Contains")
          .build();

  Map<SpatialFunction, String> SPATIAL_OPERATORS_3D =
      new ImmutableMap.Builder<SpatialFunction, String>().build();
}
