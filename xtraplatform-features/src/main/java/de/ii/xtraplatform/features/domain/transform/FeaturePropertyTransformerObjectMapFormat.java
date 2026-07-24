/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureTokenType;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.strings.domain.StringTemplateFilters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.immutables.value.Value;

@Value.Immutable
public interface FeaturePropertyTransformerObjectMapFormat
    extends FeaturePropertyTokenSliceTransformer {

  String TYPE = "OBJECT_MAP_FORMAT";

  @Override
  default String getType() {
    return TYPE;
  }

  Function<String, String> getSubstitutionLookup();

  Map<String, String> getMapping();

  @Override
  default FeatureSchema transformSchema(FeatureSchema schema) {
    checkObject(schema);

    Builder builder = new Builder().from(schema).propertyMap(Map.of());

    getMapping()
        .keySet()
        .forEach(
            key ->
                builder.putProperties2(
                    key, new ImmutableFeatureSchema.Builder().type(Type.STRING)));

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
    Function<String, String> lookup =
        getValueLookup(currentPropertyPath, getSubstitutionLookup(), slice, start, end);

    result.add(FeatureTokenType.OBJECT);
    result.add(rootPath);

    getMapping()
        .forEach(
            (key, template) -> {
              List<String> path = new ArrayList<>(rootPath);
              path.add(key);

              String value = StringTemplateFilters.applyTemplate(template, lookup);

              result.addAll(valueSlice(path, value, Type.STRING));
            });

    result.add(FeatureTokenType.OBJECT_END);
    result.add(rootPath);
  }
}
