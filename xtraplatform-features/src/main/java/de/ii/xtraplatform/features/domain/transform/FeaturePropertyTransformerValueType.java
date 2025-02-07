/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable
public interface FeaturePropertyTransformerValueType extends FeaturePropertySchemaTransformer {

  String TYPE = "VALUE_TYPE";

  @Override
  default String getType() {
    return TYPE;
  }

  @Override
  default FeatureSchema transform(String currentPropertyPath, FeatureSchema schema) {
    Type type = Type.valueOf(getParameter());
    if (Objects.equals(currentPropertyPath, getPropertyPath())
        && !Objects.equals(schema.getType(), type)) {
      return new ImmutableFeatureSchema.Builder().from(schema).type(type).build();
    }

    return schema;
  }
}
