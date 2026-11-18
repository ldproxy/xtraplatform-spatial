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
import de.ii.xtraplatform.features.domain.MappingOperationResolver;
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
    return findProperty(getObjectTables(), propertyName);
  }

  default Optional<SqlQuerySchema> getTableForValue(String propertyName) {
    return getTableForValue(propertyName, MappingRule.Scope.R);
  }

  default Optional<SqlQuerySchema> getTableForValue(String propertyName, MappingRule.Scope scope) {
    if (scope == MappingRule.Scope.W) {
      return findProperty(getWritableTables(), propertyName);
    }

    return findProperty(getValueTables(), propertyName);
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
                findProperty(
                        scope == MappingRule.Scope.W ? getWritableColumns() : getValueColumns(),
                        propertyName)
                    .map(column -> Tuple.of(table, column)));
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForId() {
    return getColumnForRole(Role.ID);
  }

  /**
   * Whether the database assigns the id of a new feature on insert: the column of the property with
   * the role {@code ID} is the primary key of its table and is not excluded from generation with
   * {@code {generated=false}}. Where the id is generated, a value in the request body is not
   * inserted, so the id of a new feature is the one that the insert returns.
   */
  default boolean hasGeneratedId() {
    Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> column = getColumnForId();

    if (column.isEmpty() || getSchemaForId().isEmpty()) {
      return true;
    }

    if (!Objects.equals(column.get().first().getPrimaryKey(), column.get().second().getName())) {
      return false;
    }

    return !column.get().second().hasOperation(SqlQueryColumn.Operation.DO_NOT_GENERATE);
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForFilterGeometry() {
    return getColumnForRole(Role.FILTER_GEOMETRY).or(() -> getColumnForRole(Role.PRIMARY_GEOMETRY));
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForPrimaryGeometry() {
    return getColumnForRole(Role.PRIMARY_GEOMETRY);
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForPrimaryInstant() {
    return getColumnForRole(Role.PRIMARY_INSTANT);
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForPrimaryIntervalStart() {
    return getColumnForRole(Role.PRIMARY_INTERVAL_START);
  }

  default Optional<Tuple<SqlQuerySchema, SqlQueryColumn>> getColumnForPrimaryIntervalEnd() {
    return getColumnForRole(Role.PRIMARY_INTERVAL_END);
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
    if (role == Role.PRIMARY_INSTANT) {
      return getMainSchema().getPrimaryInstant();
    }
    if (role == Role.PRIMARY_INTERVAL_START) {
      return getMainSchema()
          .getPrimaryInterval()
          .map(de.ii.xtraplatform.features.domain.Tuple::first);
    }
    if (role == Role.PRIMARY_INTERVAL_END) {
      return getMainSchema()
          .getPrimaryInterval()
          .map(de.ii.xtraplatform.features.domain.Tuple::second);
    }

    return Optional.empty();
  }

  default Optional<FeatureSchema> getSchemaForObject(String propertyName) {
    return findProperty(getObjectSchemas(), propertyName);
  }

  default Optional<FeatureSchema> getSchemaForValue(String propertyName) {
    if (Objects.equals(propertyName, ID_PLACEHOLDER)) {
      return getMainSchema().getIdProperty();
    }

    return findProperty(getValueSchemas(), propertyName);
  }

  /**
   * In a type with multiple main tables, the properties of each table are keyed with the index of
   * the table as prefix, e.g. `0_name`, while filters and sort keys use the plain name.
   */
  private <T> Optional<T> findProperty(Map<String, T> properties, String propertyName) {
    if (properties.containsKey(propertyName)
        || Objects.isNull(getMainSchema())
        || getMainSchema().getConcat().isEmpty()) {
      return Optional.ofNullable(properties.get(propertyName));
    }

    return properties.entrySet().stream()
        .filter(
            property ->
                MappingOperationResolver.isConcatPath(property.getKey())
                    && Objects.equals(
                        MappingOperationResolver.cleanConcatPath(property.getKey()), propertyName))
        .map(Map.Entry::getValue)
        .findFirst();
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
