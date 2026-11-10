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
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public interface FeaturePropertyTransformerObjectMapDuplicate
    extends FeaturePropertyTokenSliceTransformer {

  String TYPE = "OBJECT_MAP_DUPLICATE";

  @Override
  default String getType() {
    return TYPE;
  }

  Map<String, String> getMapping();

  @Value.Derived
  default Map<String, String> getEffectiveMapping() {
    return getMapping().entrySet().stream()
        .map(entry -> Map.entry(entry.getKey(), getPropertyPath() + "." + entry.getValue()))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  default FeatureSchema transformSchema(FeatureSchema schema) {
    checkObject(schema);

    for (String sourceKey : getEffectiveMapping().values()) {
      if (!schema.getPropertyMap().containsKey(sourceKey)) {
        throw new IllegalArgumentException(
            String.format(
                "Source key for duplication not found: %s Path: %s",
                sourceKey, schema.getFullPathAsString()));
      }
      checkValue(schema.getPropertyMap().get(sourceKey));
    }

    Builder builder = new Builder().from(schema);

    getEffectiveMapping()
        .forEach(
            (key, value) ->
                builder.putPropertyMap(
                    getPropertyPath() + "." + key,
                    new Builder()
                        .from(schema.getPropertyMap().get(value))
                        .name(key)
                        .path(List.of(key))
                        .build()));

    return builder.build();
  }

  @Override
  default void transformObject(
      String currentPropertyPath,
      List<Object> slice,
      List<String> rootPath,
      int start,
      int end,
      List<Object> result) {
    Map<String, Integer> valueIndexes = getValueIndexes(slice, start, end);

    result.addAll(slice.subList(start, end));

    List<Object> copied = new ArrayList<>();

    getEffectiveMapping()
        .forEach(
            (key, sourceKey) -> {
              if (valueIndexes.containsKey(sourceKey)) {
                Integer valueIndex = valueIndexes.get(sourceKey);
                List<String> newPath = new ArrayList<>(rootPath);
                newPath.add(key);
                String value = (String) slice.get(valueIndex);
                Type type = (Type) slice.get(valueIndex + 1);

                copied.addAll(valueSlice(newPath, value, type));
              }
            });

    result.addAll(result.size() - 2, copied);
  }
}
