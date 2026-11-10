/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public interface FeaturePropertyTransformerObjectAddConstants
    extends FeaturePropertyTokenSliceTransformer {

  String TYPE = "OBJECT_ADD_CONSTANTS";

  @Override
  default String getType() {
    return TYPE;
  }

  Map<String, String> getMapping();

  @Override
  default FeatureSchema transformSchema(FeatureSchema schema) {
    checkObject(schema);

    Builder builder = new Builder().from(schema);

    getMapping()
        .forEach(
            (key, value) ->
                builder.putPropertyMap(
                    getPropertyPath() + "." + key,
                    new Builder().name(key).path(List.of(key)).type(Type.STRING).build()));

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
    result.addAll(slice.subList(start, end));

    List<Object> copied = new ArrayList<>();

    getMapping()
        .forEach(
            (key, value) -> {
              List<String> newPath = new ArrayList<>(rootPath);
              newPath.add(key);

              copied.addAll(valueSlice(newPath, value, Type.STRING));
            });

    result.addAll(end - 2, copied);
  }
}
