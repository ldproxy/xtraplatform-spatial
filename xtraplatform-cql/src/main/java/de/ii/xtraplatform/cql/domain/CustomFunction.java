/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableCustomFunction.Builder.class)
public interface CustomFunction {

  String getName();

  @Nullable
  @Value.Default
  default String getDescription() {
    return null;
  }

  List<Cql2FunctionArgument> getArguments();

  List<String> getReturns();

  Map<String, String> getExpression();

  static CustomFunction of(
      String name, List<Cql2FunctionArgument> arguments, List<String> returns) {
    return new ImmutableCustomFunction.Builder()
        .name(name)
        .arguments(arguments)
        .returns(returns)
        .build();
  }
}
