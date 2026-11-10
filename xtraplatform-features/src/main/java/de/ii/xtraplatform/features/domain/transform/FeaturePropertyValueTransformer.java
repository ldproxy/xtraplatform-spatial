/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface FeaturePropertyValueTransformer extends FeaturePropertyTransformer<String> {

  Logger LOGGER = LoggerFactory.getLogger(FeaturePropertyValueTransformer.class);

  List<SchemaBase.Type> getSupportedPropertyTypes();

  default boolean matches(FeatureSchema schema) {
    Type valueType = schema.getValueType().orElse(schema.getType());
    boolean isTypeMatching =
        getSupportedPropertyTypes().isEmpty() || getSupportedPropertyTypes().contains(valueType);

    if (!isTypeMatching && LOGGER.isWarnEnabled()) {
      LOGGER.warn(
          "Skipping {} transformation for property '{}', type {} is not supported. Supported types: {}",
          getType(),
          getPropertyPath(),
          valueType,
          getSupportedPropertyTypes());
    }

    return isTypeMatching;
  }
}
