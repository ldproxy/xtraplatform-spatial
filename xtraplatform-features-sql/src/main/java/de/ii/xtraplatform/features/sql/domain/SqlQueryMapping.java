/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  Map<String, Integer> getValueColumnIndexes();

  Map<String, FeatureSchema> getObjectSchemas();

  Map<String, FeatureSchema> getValueSchemas();

  default Optional<SqlQuerySchema> getTableForObject(String propertyName) {
    // System.out.println("getObjectTables: " + propertyName);
    return Optional.ofNullable(getObjectTables().get(propertyName));
  }

  default Optional<SqlQuerySchema> getTableForValue(String propertyName) {
    // System.out.println("getValueTables: " + propertyName);
    return Optional.ofNullable(getValueTables().get(propertyName));
  }

  default Optional<Tuple<SqlQuerySchema, Integer>> getColumnForValue(String propertyName) {
    // System.out.println("getValueTables: " + propertyName);
    return getTableForValue(propertyName)
        .flatMap(
            table ->
                Optional.ofNullable(getValueColumnIndexes().get(propertyName))
                    .map(index -> Tuple.of(table, index)));
  }

  default Optional<FeatureSchema> getSchemaForObject(String propertyName) {
    // System.out.println("getObjectSchemas: " + propertyName);
    return Optional.ofNullable(getObjectSchemas().get(propertyName));
  }

  default Optional<FeatureSchema> getSchemaForValue(String propertyName) {
    // System.out.println("getValueSchemas: " + propertyName);
    return Optional.ofNullable(getValueSchemas().get(propertyName));
  }
}
