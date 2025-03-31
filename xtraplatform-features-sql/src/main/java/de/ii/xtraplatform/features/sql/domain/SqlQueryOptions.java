/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import de.ii.xtraplatform.features.domain.FeatureProviderConnector;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SortKey;
import de.ii.xtraplatform.features.domain.SortKey.Direction;
import de.ii.xtraplatform.features.sql.app.OnlyReturnables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public interface SqlQueryOptions extends FeatureProviderConnector.QueryOptions {

  static SqlQueryOptions withColumnTypes(Class<?>... columnTypes) {
    return withColumnTypes(Arrays.asList(columnTypes));
  }

  static SqlQueryOptions single() {
    return withColumnTypes(String.class);
  }

  static SqlQueryOptions tuple() {
    return withColumnTypes(String.class, String.class);
  }

  static SqlQueryOptions ddl() {
    return withColumnTypes();
  }

  static SqlQueryOptions ignoreResults() {
    return withColumnTypes();
  }

  static SqlQueryOptions mutation() {
    return withColumnTypes();
  }

  static SqlQueryOptions withColumnTypes(List<Class<?>> columnTypes) {
    return new ImmutableSqlQueryOptions.Builder().customColumnTypes(columnTypes).build();
  }

  Optional<SchemaSql> getTableSchema();

  Optional<String> getType();

  List<SortKey> getCustomSortKeys();

  List<Class<?>> getCustomColumnTypes();

  @Value.Default
  default boolean isHitsOnly() {
    return false;
  }

  @Value.Default
  default int getContainerPriority() {
    return 0;
  }

  @Value.Default
  default int getChunkSize() {
    return 1000;
  }

  @Value.Default
  default boolean isGeometryWkb() {
    return false;
  }

  @Value.Derived
  default Optional<SchemaSql> getTableSchemaReturnables() {
    return getTableSchema().map(schema -> schema.accept(new OnlyReturnables()));
  }

  @Value.Derived
  default List<String> getSortKeys() {
    return Stream.concat(
            getCustomSortKeys().stream().map(SortKey::getField),
            getTableSchema()
                .map(attributesContainer -> attributesContainer.getSortKeys().stream())
                .orElse(Stream.empty()))
        .collect(Collectors.toList());
  }

  @Value.Derived
  default List<SortKey.Direction> getSortDirections() {
    return Stream.concat(
            getCustomSortKeys().stream().map(SortKey::getDirection),
            getTableSchema()
                .map(
                    attributesContainer ->
                        attributesContainer.getSortKeys().stream().map(s -> Direction.ASCENDING))
                .orElse(Stream.empty()))
        .collect(Collectors.toList());
  }

  @Value.Derived
  default List<Class<?>> getColumnTypes() {
    List<Class<?>> columnTypes = new ArrayList<>();

    getTableSchema()
        .ifPresent(
            attributesContainer ->
                attributesContainer.getProperties().stream()
                    .filter(SchemaBase::isValue)
                    .forEach(
                        attribute ->
                            columnTypes.add(
                                attribute.isSpatial() && isGeometryWkb()
                                    ? byte[].class
                                    : String.class)));

    columnTypes.addAll(getCustomColumnTypes());

    return columnTypes;
  }

  @Value.Derived
  default boolean isPlain() {
    return !getTableSchema().isPresent();
  }
}
