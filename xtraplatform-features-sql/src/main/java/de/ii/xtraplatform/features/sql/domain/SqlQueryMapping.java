/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import static de.ii.xtraplatform.cql.domain.In.ID_PLACEHOLDER;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.MappingRule;
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable(lazyhash = true)
@Value.Style(
    builder = "new",
    get = {"is*", "get*"}) // , deepImmutablesDetection = true, attributeBuilderDetection = true)
@JsonDeserialize(builder = ImmutableSqlQueryMapping.Builder.class)
public interface SqlQueryMapping {

  List<SqlQuerySchema> getTables();

  @Value.Lazy
  default SqlQuerySchema getMainTable() {
    return getTables().get(0);
  }

  @Nullable
  FeatureSchema getMainSchema();

  Map<String, SqlQuerySchema> getObjectTables();

  Map<String, SqlQuerySchema> getValueTables();

  Map<String, SqlQueryColumn> getValueColumns();

  Map<String, SqlQuerySchema> getWritableTables();

  Map<String, SqlQueryColumn> getWritableColumns();

  Map<String, FeatureSchema> getObjectSchemas();

  Map<String, FeatureSchema> getValueSchemas();

  default Optional<SqlQuerySchema> getTableForObject(String propertyName) {
    // System.out.println("getObjectTables: " + propertyName);
    return Optional.ofNullable(getObjectTables().get(propertyName));
  }

  default Optional<SqlQuerySchema> getTableForValue(String propertyName) {
    return getTableForValue(propertyName, MappingRule.Scope.R);
  }

  default Optional<SqlQuerySchema> getTableForValue(String propertyName, MappingRule.Scope scope) {
    if (scope == MappingRule.Scope.W) {
      return Optional.ofNullable(getWritableTables().get(propertyName));
    }

    return Optional.ofNullable(getValueTables().get(propertyName));
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForValue(String propertyName) {
    return getColumnForValue(propertyName, MappingRule.Scope.R);
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForValue(
      String propertyName, MappingRule.Scope scope) {
    if (Objects.equals(propertyName, ID_PLACEHOLDER)) {
      return getColumnForId();
    }

    return getTableForValue(propertyName, scope)
        .flatMap(
            table ->
                Optional.ofNullable(
                        (scope == MappingRule.Scope.W ? getWritableColumns() : getValueColumns())
                            .get(propertyName))
                    .map(index -> Tuple.of(table, index)));
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForId() {
    return getColumnForRole(Role.ID);
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForPrimaryGeometry() {
    return getColumnForRole(Role.PRIMARY_GEOMETRY);
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForRole(Role role) {
    return getTables().stream()
        .flatMap(
            table -> {
              for (int i = 0; i < table.getColumns().size(); i++) {
                if (table.getColumns().get(i).getRole().filter(r -> r == role).isPresent()) {
                  return Stream.of(Tuple.of(table, table.getColumns().get(i)));
                }
              }
              return Stream.empty();
            })
        .findFirst();
  }

  default Optional<FeatureSchema> getSchemaForId() {
    return getSchemaForRole(Role.ID);
  }

  default Optional<FeatureSchema> getSchemaForPrimaryGeometry() {
    return getSchemaForRole(Role.PRIMARY_GEOMETRY);
  }

  default Optional<FeatureSchema> getSchemaForRole(Role role) {
    if (role == Role.ID) {
      return getMainSchema().getIdProperty();
    }
    if (role == Role.PRIMARY_GEOMETRY) {
      return getMainSchema().getPrimaryGeometry();
    }

    return Optional.empty();
  }

  default Optional<FeatureSchema> getSchemaForObject(String propertyName) {
    return Optional.ofNullable(getObjectSchemas().get(propertyName));
  }

  default Optional<FeatureSchema> getSchemaForValue(String propertyName) {
    if (Objects.equals(propertyName, ID_PLACEHOLDER)) {
      return getMainSchema().getIdProperty();
    }

    return Optional.ofNullable(getValueSchemas().get(propertyName));
  }

  String IN_CONNECTED_ARRAY = "IN_CONNECTED_ARRAY";
  String PATH_IN_CONNECTOR = "PATH_IN_CONNECTOR";

  default boolean isInConnectedArray(FeatureSchema schema) {
    return Boolean.parseBoolean(schema.getAdditionalInfo().get(IN_CONNECTED_ARRAY));
  }

  default String getPathInConnector(FeatureSchema schema) {
    return schema.getAdditionalInfo().getOrDefault(PATH_IN_CONNECTOR, schema.getName());
  }
}
