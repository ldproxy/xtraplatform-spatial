/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.sql.domain.SqlQueryTable.DefaultsFilter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable(lazyhash = true)
@Value.Style(
    builder = "new",
    get = {"is*", "get*"}) // , deepImmutablesDetection = true, attributeBuilderDetection = true)
@JsonDeserialize(builder = ImmutableSqlQueryColumn.Builder.class)
public interface SqlQueryColumn {
  enum Operation {
    WKT,
    WKB,
    FORCE_POLYGON_CCW,
    LINEARIZE_CURVES,
    DATE,
    DATETIME,
    CONSTANT,
    EXPRESSION,
    CONNECTOR,
    DO_NOT_GENERATE;
  }

  String getName();

  String getPathSegment();

  SchemaBase.Type getType();

  Optional<SchemaBase.Role> getRole();

  Map<Operation, String[]> getOperations();

  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = DefaultsFilter.class)
  int getSchemaIndex();

  default boolean hasOperation(Operation operation) {
    return getOperations().containsKey(operation);
  }

  default List<String> getOperationParameters(Operation operation) {
    if (!getOperations().containsKey(operation)) {
      return List.of();
    }
    return Arrays.stream(getOperations().get(operation)).toList();
  }

  default Optional<String> getOperationParameter(Operation operation) {
    if (!getOperations().containsKey(operation)) {
      return Optional.empty();
    }
    return Arrays.stream(getOperations().get(operation)).findFirst();
  }

  default String getOperationParameter(Operation operation, String defaultValue) {
    if (!getOperations().containsKey(operation)) {
      return defaultValue;
    }
    return Arrays.stream(getOperations().get(operation)).findFirst().orElse(defaultValue);
  }

  class DefaultsFilter {
    @Override
    public boolean equals(Object value) {
      return Objects.equals(value, 0);
    }
  }
}
