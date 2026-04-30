/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.graphql.app;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaVisitorTopDown;
import de.ii.xtraplatform.features.graphql.domain.GraphQlQueries;
import de.ii.xtraplatform.strings.domain.StringTemplateFilters;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SchemaDeriverGraphQl implements SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {

  private final Map<String, FeatureSchema> types;
  private final GraphQlQueries queries;

  public SchemaDeriverGraphQl(Map<String, FeatureSchema> types, GraphQlQueries queries) {
    this.types = types;
    this.queries = queries;
  }

  @Override
  public FeatureSchema visit(
      FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {

    Map<String, FeatureSchema> visitedPropertiesMap =
        asMap(visitedProperties, FeatureSchema::getName);

    Optional<String> sourcePath = schema.getSourcePath();

    if (schema.isSimpleFeatureGeometry()) {
      sourcePath =
          sourcePath.flatMap(
              path ->
                  queries
                      .getCollection()
                      .getFields()
                      .getGeometry()
                      .map(
                          template ->
                              StringTemplateFilters.applyTemplate(
                                  template, (String key) -> Map.of("sourcePath", path).get(key)))
                      .map(expr -> FilterEncoderGraphQl.fromString(expr, true))
                      .map(expr -> path + "/" + expr.get(path)));
    } else if (schema.isFeatureRef()) {
      Optional<String> id =
          schema
              .getRefType()
              .flatMap(refType -> Optional.ofNullable(types.get(refType)))
              .flatMap(FeatureSchema::getIdProperty)
              .flatMap(FeatureSchema::getSourcePath);
      if (id.isPresent()) {
        sourcePath = sourcePath.map(path -> path + "/" + id.get());
      }
    }

    return new ImmutableFeatureSchema.Builder()
        .from(schema)
        .sourcePath(sourcePath)
        .propertyMap(visitedPropertiesMap)
        .build();
  }
}
