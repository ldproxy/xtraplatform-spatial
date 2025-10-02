/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Scope;
import de.ii.xtraplatform.features.domain.SchemaVisitorTopDown;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class WithScope implements SchemaVisitorTopDown<FeatureSchema, FeatureSchema> {

  private final Set<Scope> scopes;

  public WithScope(Scope scope) {
    this.scopes = EnumSet.of(scope);
  }

  public WithScope(Set<Scope> scopes) {
    this.scopes = scopes;
  }

  @Override
  public FeatureSchema visit(
      FeatureSchema schema, List<FeatureSchema> parents, List<FeatureSchema> visitedProperties) {

    // always include ID property
    if (!schema.hasOneOf(scopes)
        && !schema.isId()
        && !schema.isEmbeddedId()
        && (!schema.isObject()
            || scopes.contains(Scope.RETURNABLE)
            || scopes.contains(Scope.RECEIVABLE))) {
      return null;
    }

    Map<String, FeatureSchema> visitedPropertiesMap =
        visitedProperties.stream()
            .filter(Objects::nonNull)
            .map(
                featureSchema ->
                    new SimpleImmutableEntry<>(featureSchema.getFullPathAsString(), featureSchema))
            .collect(
                ImmutableMap.toImmutableMap(
                    Entry::getKey, Entry::getValue, (first, second) -> second));
    List<FeatureSchema> visitedConcat =
        schema.getConcat().stream()
            .map(concatSchema -> concatSchema.accept(this, parents))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    List<FeatureSchema> visitedCoalesce =
        schema.getCoalesce().stream()
            .map(coalesceSchema -> coalesceSchema.accept(this, parents))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (schema.isObject()
        && !parents.isEmpty()
        && visitedProperties.isEmpty()
        && visitedConcat.isEmpty()
        && visitedCoalesce.isEmpty()) {
      return null;
    }

    return new ImmutableFeatureSchema.Builder()
        .from(schema)
        .propertyMap(visitedPropertiesMap)
        .concat(visitedConcat)
        .coalesce(visitedCoalesce)
        .build();
  }
}
