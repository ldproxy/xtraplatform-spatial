/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import de.ii.xtraplatform.cql.domain.Operation;
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Value;

public interface SqlQueryTable {
  String getName();

  String getPathSegment();

  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = DefaultsFilter.class)
  @Value.Default
  default String getSortKey() {
    return "id";
  }

  Optional<Operation<?>> getFilter();

  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = DefaultsFilter.class)
  @Value.Default
  default boolean isSortKeyUnique() {
    return true;
  }

  class DefaultsFilter {
    @Override
    public boolean equals(Object value) {
      return Objects.equals(value, true) || Objects.equals(value, "id");
    }
  }
}
