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

  private CqlBuiltInFunctions() {}

  public static final List<CustomFunction> FUNCTIONS =
      ImmutableList.of(
          CustomFunction.of(
              "UPPER",
              "Converts a string to uppercase.",
              ImmutableList.of(argument("value", "Input text.", "STRING")),
              ImmutableList.of("STRING"),
              Map.of("SQL", "UPPER($value)")),
          CustomFunction.of(
              "LOWER",
              "Converts a string to lowercase.",
              ImmutableList.of(argument("value", "Input text.", "STRING")),
              ImmutableList.of("STRING"),
              Map.of("SQL", "LOWER($value)")),
          CustomFunction.of(
              "POSITION",
              "Returns the row position in nested filters.",
              ImmutableList.of(),
              ImmutableList.of("INTEGER"),
              Map.of("SQL", "%1$srow_number%2$s")),
          CustomFunction.of(
              "DIAMETER2D",
              "Returns the 2D diameter of a geometry.",
              ImmutableList.of(argument("geometry", "Input geometry.", "GEOMETRY")),
              ImmutableList.of("FLOAT"),
              Map.of(
                  "SQL/PGIS",
                  "ST_Length(ST_BoundingDiagonal(Box2D(ST_Transform($geometry,3857))))")),
          CustomFunction.of(
              "DIAMETER3D",
              "Returns the 3D diameter of a geometry.",
              ImmutableList.of(argument("geometry", "Input geometry.", "GEOMETRY")),
              ImmutableList.of("FLOAT"),
              Map.of(
                  "SQL/PGIS",
                  "ST_3DLength(ST_BoundingDiagonal(Box3D(ST_Transform(ST_Force3DZ($geometry),4978))))")),
          CustomFunction.of(
              "ALIKE",
              "Checks if one of the array values matches the LIKE pattern.",
              ImmutableList.of(
                  argument("values", "Array value expression.", "VALUE_ARRAY"),
                  argument("pattern", "LIKE pattern.", "STRING")),
              ImmutableList.of("BOOLEAN"),
              Map.of(
                  "SQL/PGIS", "$values::varchar LIKE $pattern",
                  "SQL/GPKG", "cast($values as text) LIKE $pattern")));

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
