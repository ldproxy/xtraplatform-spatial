/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.features.domain.SchemaBase;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface FeaturePropertyTransformerNullValue extends FeaturePropertyValueTransformer {

  String TYPE = "NULL_VALUE";

  @Override
  default String getType() {
    return TYPE;
  }

  @Override
  default List<SchemaBase.Type> getSupportedPropertyTypes() {
    return ImmutableList.of(SchemaBase.Type.STRING, SchemaBase.Type.INTEGER, SchemaBase.Type.FLOAT);
  }

  @Override
  default String transform(String currentPropertyPath, String input) {
    if (input.matches(getParameter())) {
      return null;
    }

    return input;
  }
}
