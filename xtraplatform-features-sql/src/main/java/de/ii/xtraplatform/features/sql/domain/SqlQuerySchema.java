/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable(lazyhash = true)
@Value.Style(
    builder = "new",
    get = {"is*", "get*"}) // , deepImmutablesDetection = true, attributeBuilderDetection = true)
@JsonDeserialize(builder = ImmutableSqlQuerySchema.Builder.class)
public interface SqlQuerySchema extends SqlQueryTable {

  List<SqlQueryJoin> getRelations();

  List<String> getColumns();

  enum Operation {
    WKT,
    FORCE_POLYGON_CCW,
    LINEARIZE_CURVES,
    DATE,
    DATETIME,
    CONSTANT,
    EXPRESSION,
  }

  Map<Integer, Map<Operation, String>> getColumnOperations();

  @JsonIgnore
  @Value.Lazy
  default List<SqlQueryTable> asTablePath() {
    return Stream.concat(getRelations().stream(), Stream.of(this)).toList();
  }
}
