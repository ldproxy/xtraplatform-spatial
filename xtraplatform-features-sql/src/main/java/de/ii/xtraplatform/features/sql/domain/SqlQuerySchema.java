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
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import java.util.ArrayList;
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
  List<SqlQueryColumn> getWritableColumns();

  Optional<SchemaBase.Role> getRole();

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
    // TODO: generalize
    if (getRelations().size() == 2) {
      // There is a junction table, so we return the path of the parent table
      return getFullPath().subList(0, 1);
    }
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

  @JsonIgnore
  @Value.Lazy
  default List<String> getSortKeys() {
    List<String> keys = new ArrayList<>();
    String prefix = "";

    for (SqlQueryJoin join : getRelations()) {
      prefix += join.getPathSegment();
      if (!join.isJunction()) {
        keys.add(String.format("%s.%s", prefix, join.getSortKey()));
      }
    }
    prefix += this.getPathSegment();

    keys.add(String.format("%s.%s", prefix, this.getSortKey()));

    return keys;
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

  @JsonIgnore
  @Value.Lazy
  default boolean isRoot() {
    return getRelations().isEmpty();
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isOne2One() {
    return getRelations().size() == 1
        && Objects.equals(
            getRelations().get(0).getTargetField(), getSortKey() /*TODO getPrimaryKey()*/);
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isOne2N() {
    return getRelations().size() == 1 && !isOne2One();
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isM2N() {
    return getRelations().size() == 2 && getRelations().get(1).isJunction();
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isReference() {
    return getRole().isPresent() && getRole().get() == Role.FEATURE_REF;
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isOne2OneWithForeignKey() {
    if (getRelations().size() != 1) {
      return false;
    }

    SqlQueryJoin join = getRelations().get(0);

    return !Objects.equals(join.getSortKey(), join.getSourceField());
  }
}
