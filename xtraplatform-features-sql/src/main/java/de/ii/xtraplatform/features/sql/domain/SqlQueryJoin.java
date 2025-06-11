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
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable(lazyhash = true)
@Value.Style(
    builder = "new",
    get = {"is*", "get*"}) // , deepImmutablesDetection = true, attributeBuilderDetection = true)
@JsonDeserialize(builder = ImmutableSqlQueryJoin.Builder.class)
public interface SqlQueryJoin extends SqlQueryTable {

  String getSourceField();

  String getTarget();

  String getTargetField();

  Optional<String> getTargetFilter();

  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = DefaultsFilter.class)
  @Value.Default
  default SqlPath.JoinType getJoinType() {
    return SqlPath.JoinType.INNER;
  }

  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = DefaultsFilter.class)
  @Value.Default
  default boolean isJunction() {
    return false;
  }

  class DefaultsFilter {
    @Override
    public boolean equals(Object value) {
      return Objects.equals(value, false) || Objects.equals(value, SqlPath.JoinType.INNER);
    }
  }
}
