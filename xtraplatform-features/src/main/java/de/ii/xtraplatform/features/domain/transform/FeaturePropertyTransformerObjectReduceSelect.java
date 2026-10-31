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
import de.ii.xtraplatform.features.domain.Tuple;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface FeaturePropertyTransformerObjectReduceSelect
    extends FeaturePropertyTokenSliceTransformer {

  String TYPE = "OBJECT_REDUCE_SELECT";

  @Override
  default String getType() {
    return TYPE;
  }

  @Override
  default FeatureSchema transformSchema(FeatureSchema schema) {
    checkObject(schema);

    Optional<FeatureSchema> selected = findProperty(schema, getParameter());

    if (selected.isEmpty()) {
      throw new IllegalArgumentException("Selected property not found: " + getParameter());
    }

    String mergedSourcePath =
        schema.getSourcePath().map(s -> s + "/").orElse("")
            + selected.get().getSourcePath().orElse("");

    return new Builder()
        .from(selected.get())
        .type(schema.isArray() ? Type.VALUE_ARRAY : Type.VALUE)
        .valueType(selected.get().getValueType().orElse(selected.get().getType()))
        .name(schema.getName())
        .sourcePath(mergedSourcePath)
        .path(schema.getPath())
        .parentPath(schema.getParentPath())
        .build();
  }

  @Override
  default void transformObject(
      String currentPropertyPath,
      List<Object> slice,
      List<String> rootPath,
      int start,
      int end,
      List<Object> result) {
    Tuple<String, Type> valueAndType =
        getValueAndType(slice, getPropertyPath() + "." + getParameter(), start, end);

    result.addAll(valueSlice(rootPath, valueAndType.first(), valueAndType.second()));
  }
}
