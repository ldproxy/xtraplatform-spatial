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
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableCustomFunction.Builder.class)
public interface CustomFunction {

  String getName();

  List<String> getArgumentTypes();

  String getReturnType();

  @Nullable
  @Value.Default
  default String getSqlExpression() {
    return null;
  }

  static CustomFunction of(String name, List<String> argumentTypes, String returnType) {
    return new ImmutableCustomFunction.Builder()
        .name(name)
        .argumentTypes(argumentTypes)
        .returnType(returnType)
        .build();
  }
}
