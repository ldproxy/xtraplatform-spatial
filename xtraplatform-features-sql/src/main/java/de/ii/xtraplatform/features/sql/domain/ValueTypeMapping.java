/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.sql.JDBCType;
import java.sql.SQLType;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public final class ValueTypeMapping {

  private static final List<SQLType> SQL_BOOLEANS = ImmutableList.of(JDBCType.BOOLEAN);

  private static final List<SQLType> SQL_INTEGERS =
      ImmutableList.of(JDBCType.BIGINT, JDBCType.INTEGER, JDBCType.SMALLINT, JDBCType.TINYINT);

  private static final List<SQLType> SQL_FLOATS =
      ImmutableList.of(
          JDBCType.DECIMAL, JDBCType.DOUBLE, JDBCType.FLOAT, JDBCType.NUMERIC, JDBCType.REAL);

  private static final List<SQLType> SQL_STRINGS =
      ImmutableList.of(
          JDBCType.CHAR,
          JDBCType.LONGNVARCHAR,
          JDBCType.LONGVARCHAR,
          JDBCType.NCHAR,
          JDBCType.NVARCHAR,
          JDBCType.VARCHAR);

  private static final List<SQLType> SQL_TIMESTAMPS =
      Stream.concat(
              Stream.of(JDBCType.TIMESTAMP, JDBCType.TIMESTAMP_WITH_TIMEZONE, JDBCType.DATE),
              SQL_STRINGS.stream())
          .collect(ImmutableList.toImmutableList());

  private static final List<SQLType> SQL_GEOMETRIES =
      ImmutableList.of(
          new SQLType() {
            @Override
            public String getName() {
              return "geometry";
            }

            @Override
            public String getVendor() {
              return JDBCType.OTHER.getVendor();
            }

            @Override
            public Integer getVendorTypeNumber() {
              return JDBCType.OTHER.getVendorTypeNumber();
            }
          });

  private static final Map<Type, List<SQLType>> MAPPINGS =
      new ImmutableMap.Builder<Type, List<SQLType>>()
          .put(Type.BOOLEAN, SQL_BOOLEANS)
          .put(Type.INTEGER, SQL_INTEGERS)
          .put(Type.FLOAT, SQL_FLOATS)
          .put(Type.STRING, SQL_STRINGS)
          .put(Type.DATETIME, SQL_TIMESTAMPS)
          .put(Type.GEOMETRY, SQL_GEOMETRIES)
          .build();

  private ValueTypeMapping() {}

  public static List<SQLType> getSourceTypes(Type type) {
    return MAPPINGS.getOrDefault(type, ImmutableList.of());
  }

  public static boolean matches(SQLType sqlType, String databaseSpecificTypeName, Type type) {
    return getSourceTypes(type).stream()
        .anyMatch(
            allowedType -> {
              boolean vendorTypeNumberMatches =
                  Objects.equals(allowedType.getVendorTypeNumber(), sqlType.getVendorTypeNumber());

              if (vendorTypeNumberMatches
                  && Objects.equals(
                      JDBCType.OTHER.getVendorTypeNumber(), sqlType.getVendorTypeNumber())) {
                return allowedType.getName().equalsIgnoreCase(databaseSpecificTypeName);
              }

              return vendorTypeNumberMatches;
            });
  }
}
