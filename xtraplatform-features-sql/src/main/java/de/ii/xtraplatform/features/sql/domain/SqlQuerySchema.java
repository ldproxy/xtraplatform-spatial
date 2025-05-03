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
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable(lazyhash = true)
@Value.Style(
    builder = "new",
    get = {"is*", "get*"}) // , deepImmutablesDetection = true, attributeBuilderDetection = true)
@JsonDeserialize(builder = ImmutableSqlQuerySchema.Builder.class)
public interface SqlQuerySchema extends SqlQueryTable {

  List<SqlQueryJoin> getRelations();

  List<SqlQueryColumn> getColumns();

  List<SqlQueryColumn> getFilterColumns();

  @JsonIgnore
  @Value.Lazy
  default List<SqlQueryTable> asTablePath() {
    return Stream.concat(getRelations().stream(), Stream.of(this)).toList();
  }

  @JsonIgnore
  @Value.Lazy
  default List<String> getFullPath() {
    return Stream.concat(
            getRelations().stream().map(SqlQueryTable::getPathSegment),
            Stream.of(this.getPathSegment()))
        .toList();
  }

  @JsonIgnore
  @Value.Lazy
  default String getFullPathAsString() {
    return getFullPath().stream().collect(Collectors.joining("/", "/", ""));
  }

  @JsonIgnore
  @Value.Lazy
  default List<String> getParentPath() {
    return getFullPath().subList(0, getFullPath().size() - 1);
  }

  @JsonIgnore
  @Value.Lazy
  default List<List<String>> getColumnPaths() {
    return getColumns().stream().map(this::getColumnPath).toList();
  }

  default List<String> getColumnPath(SqlQueryColumn column) {
    return Stream.concat(getFullPath().stream(), Stream.of(column.getPathSegment())).toList();
  }

  /*@JsonIgnore
  @Value.Lazy
  default List<String> getColumnNames() {
    return getColumns().stream().map(SqlQueryColumn::getName).toList();
  }*/

  @JsonIgnore
  @Value.Lazy
  default List<String> getSortKeys() {
    return Stream.concat(getRelations().stream().filter(rel -> !rel.isJunction()), Stream.of(this))
        .map(
            sqlQueryTable ->
                String.format("%s.%s", sqlQueryTable.getName(), sqlQueryTable.getSortKey()))
        .toList();
  }

  @JsonIgnore
  @Value.Lazy
  default boolean hasReadableColumns() {
    return !getColumns().isEmpty();
  }

  default Optional<SqlQueryColumn> findColumn(String name) {
    return Stream.concat(getColumns().stream(), getFilterColumns().stream())
        .filter(column -> Objects.equals(column.getName(), name))
        .findFirst();
  }
}
