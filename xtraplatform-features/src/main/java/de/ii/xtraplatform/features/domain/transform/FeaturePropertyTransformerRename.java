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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable
public interface FeaturePropertyTransformerRename extends FeaturePropertySchemaTransformer {

  String TYPE = "RENAME";

  @Override
  default String getType() {
    return TYPE;
  }

  @Value.Default
  default boolean pathOnly() {
    return false;
  }

  @Override
  default FeatureSchema transform(String currentPropertyPath, FeatureSchema schema) {
    if (Objects.equals(currentPropertyPath, getPropertyPath())
        && !Objects.equals(schema.getName(), getParameter())) {
      return new ImmutableFeatureSchema.Builder()
          .from(schema)
          .name(pathOnly() ? schema.getName() : getParameter())
          .path(List.of(getParameter()))
          .propertyMap(
              adjustProperties(
                  schema.getPropertyMap(),
                  merge(schema.getParentPath(), getParameter()),
                  schema.getName(),
                  pathOnly() ? schema.getName() : getParameter()))
          .build();
    }

    return schema;
  }

  default Map<String, FeatureSchema> adjustProperties(
      Map<String, FeatureSchema> properties, List<String> parentPath, String name, String newName) {
    return properties.entrySet().stream()
        .map(
            entry ->
                Map.entry(
                    entry.getKey().replace(name, newName),
                    new ImmutableFeatureSchema.Builder()
                        .from(entry.getValue())
                        .parentPath(parentPath)
                        .propertyMap(
                            adjustProperties(
                                entry.getValue().getPropertyMap(),
                                merge(parentPath, entry.getValue().getName()),
                                "",
                                ""))
                        .build()))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static List<String> merge(List<String> path, String name) {
    List<String> merged = new ArrayList<>(path);
    merged.add(name);
    return merged;
  }
}
