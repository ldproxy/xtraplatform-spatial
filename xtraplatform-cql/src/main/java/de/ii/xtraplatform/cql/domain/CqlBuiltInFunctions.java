/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;

public final class CqlBuiltInFunctions {

  public static final String TYPE_STRING = "STRING";
  public static final String TYPE_INTEGER = "INTEGER";
  public static final String TYPE_FLOAT = "FLOAT";
  public static final String TYPE_GEOMETRY = "GEOMETRY";
  public static final String TYPE_VALUE_ARRAY = "VALUE_ARRAY";
  public static final String TYPE_BOOLEAN = "BOOLEAN";
  public static final String TYPE_DATETIME = "DATETIME";
  public static final String DIALECT_SQL = "SQL";

  public static final List<CustomFunction> FUNCTIONS =
      ImmutableList.of(
          CustomFunction.of(
              "UPPER",
              "Converts a string to uppercase.",
              ImmutableList.of(argument("value", "Input text.", TYPE_STRING)),
              ImmutableList.of(TYPE_STRING),
              Map.of(DIALECT_SQL, "UPPER($value)")),
          CustomFunction.of(
              "LOWER",
              "Converts a string to lowercase.",
              ImmutableList.of(argument("value", "Input text.", TYPE_STRING)),
              ImmutableList.of(TYPE_STRING),
              Map.of(DIALECT_SQL, "LOWER($value)")),
          CustomFunction.of(
              "POSITION",
              "Returns the row position in nested filters.",
              ImmutableList.of(),
              ImmutableList.of(TYPE_INTEGER),
              Map.of(DIALECT_SQL, "%1$srow_number%2$s")),
          CustomFunction.of(
              "NOW",
              "Returns the current timestamp.",
              ImmutableList.of(),
              ImmutableList.of(TYPE_DATETIME),
              Map.of(DIALECT_SQL, "CURRENT_TIMESTAMP")),
          CustomFunction.of(
              "DIAMETER2D",
              "Returns the 2D diameter of a geometry.",
              ImmutableList.of(argument("geometry", "Input geometry.", TYPE_GEOMETRY)),
              ImmutableList.of(TYPE_FLOAT),
              Map.of(
                  "SQL/PGIS",
                  "ST_Length(ST_BoundingDiagonal(Box2D(ST_Transform($geometry,3857))))")),
          CustomFunction.of(
              "DIAMETER3D",
              "Returns the 3D diameter of a geometry.",
              ImmutableList.of(argument("geometry", "Input geometry.", TYPE_GEOMETRY)),
              ImmutableList.of(TYPE_FLOAT),
              Map.of(
                  "SQL/PGIS",
                  "ST_3DLength(ST_BoundingDiagonal(Box3D(ST_Transform(ST_Force3DZ($geometry),4978))))")),
          CustomFunction.of(
              "ALIKE",
              "Checks if one of the array values matches the LIKE pattern.",
              ImmutableList.of(
                  argument("values", "Array value expression.", TYPE_VALUE_ARRAY),
                  argument("pattern", "LIKE pattern.", TYPE_STRING)),
              ImmutableList.of(TYPE_BOOLEAN),
              Map.of(
                  "SQL/PGIS", "$values::varchar LIKE $pattern",
                  "SQL/GPKG", "cast($values as text) LIKE $pattern")),
          CustomFunction.ofQueryExpressionOnly(
              InResultSet.TYPE,
              "Tests whether the value of a property, or the feature id, is contained in a named "
                  + "result set. Result sets are defined by other queries of the same query "
                  + "expression; this function can therefore only be used within a query "
                  + "expression, not in a standalone CQL2 filter.",
              ImmutableList.of(
                  argument(
                      "value",
                      "Property to test: a feature reference, a value property holding feature "
                          + "ids, or the feature id.",
                      TYPE_STRING),
                  argument("resultSet", "Name of the result set.", TYPE_STRING)),
              ImmutableList.of(TYPE_BOOLEAN)));

  private CqlBuiltInFunctions() {}

  public static List<CustomFunction> prependBuiltInFunctions(List<CustomFunction> customFunctions) {
    return ImmutableList.<CustomFunction>builder()
        .addAll(FUNCTIONS)
        .addAll(customFunctions)
        .build();
  }

  private static Cql2FunctionArgument argument(String name, String description, String... types) {
    return new ImmutableCql2FunctionArgument.Builder()
        .name(name)
        .description(description)
        .type(List.of(types))
        .build();
  }
}
