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

public final class CqlBuiltInFunctions {

  private CqlBuiltInFunctions() {}

  public static final List<CustomFunction> FUNCTIONS =
      ImmutableList.of(
          CustomFunction.of(
              "UPPER",
              "Converts a string to uppercase.",
              ImmutableList.of(argument("STRING")),
              ImmutableList.of("STRING"),
              java.util.Map.of()),
          CustomFunction.of(
              "LOWER",
              "Converts a string to lowercase.",
              ImmutableList.of(argument("STRING")),
              ImmutableList.of("STRING"),
              java.util.Map.of()),
          CustomFunction.of(
              "POSITION",
              "Returns the row position in nested filters.",
              ImmutableList.of(),
              ImmutableList.of("INTEGER"),
              java.util.Map.of()),
          CustomFunction.of(
              "DIAMETER2D",
              "Returns the 2D diameter of a geometry.",
              ImmutableList.of(argument("GEOMETRY")),
              ImmutableList.of("FLOAT"),
              java.util.Map.of()),
          CustomFunction.of(
              "DIAMETER3D",
              "Returns the 3D diameter of a geometry.",
              ImmutableList.of(argument("GEOMETRY")),
              ImmutableList.of("FLOAT"),
              java.util.Map.of()),
          CustomFunction.of(
              "ALIKE",
              "Checks if one of the array values matches the LIKE pattern.",
              ImmutableList.of(argument("VALUE_ARRAY"), argument("STRING")),
              ImmutableList.of("BOOLEAN"),
              java.util.Map.of()));

  public static List<CustomFunction> prependBuiltInFunctions(List<CustomFunction> customFunctions) {
    return ImmutableList.<CustomFunction>builder()
        .addAll(FUNCTIONS)
        .addAll(customFunctions)
        .build();
  }

  private static Cql2FunctionArgument argument(String... types) {
    return new ImmutableCql2FunctionArgument.Builder().type(List.of(types)).build();
  }
}
