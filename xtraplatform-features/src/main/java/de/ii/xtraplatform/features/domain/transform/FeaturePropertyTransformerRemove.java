/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public interface FeaturePropertyTransformerRemove extends FeaturePropertySchemaTransformer {

  Logger LOGGER = LoggerFactory.getLogger(FeaturePropertyTransformerRemove.class);

  enum Condition {
    ALWAYS,
    IN_COLLECTION,
    NEVER
  }

  String TYPE = "REMOVE";

  Set<String> CONDITION_VALUES =
      Arrays.stream(Condition.values()).map(Enum::name).collect(Collectors.toSet());

  @Override
  default String getType() {
    return TYPE;
  }

  boolean inCollection();

  @Override
  default FeatureSchema transform(String currentPropertyPath, FeatureSchema schema) {
    String parameter = getParameter().toUpperCase(Locale.ROOT);
    Condition condition;

    try {
      condition = Condition.valueOf(parameter);
    } catch (IllegalArgumentException e) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "Skipping {} transformation for property '{}', condition '{}' is not supported. Supported types: {}",
            getType(),
            getPropertyPath(),
            getParameter(),
            Condition.values());
      }
      return schema;
    }

    if (condition == Condition.ALWAYS
        || condition == Condition.IN_COLLECTION
            && inCollection()
            && currentPropertyPath.startsWith(getPropertyPath())) {
      return null;
    }

    return schema;
  }
}
