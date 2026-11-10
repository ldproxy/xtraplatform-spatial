/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.FeatureProviderCapabilities.Operation;
import java.util.Map;

public interface FeatureQueryEncoder<T, V extends FeatureProviderConnector.QueryOptions> {

  String PROPERTY_NOT_AVAILABLE = "PROPERTY_NOT_AVAILABLE";

  T encode(Query query, Map<String, String> additionalQueryParameters);

  V getOptions(TypeQuery typeQuery, Query query);

  FeatureProviderCapabilities getCapabilities();

  // TODO: validate filters // NOPMD
  // NOTE: anything else?
  default void validate(TypeQuery typeQuery, Query query) {
    if (query.hitsOnly() && !getCapabilities().supportsQueryOp(Operation.COUNTING)) {
      throw new IllegalArgumentException(
          String.format(
              "Feature provider has level %s,  below level %s do not support COUNT",
              FeatureProviderCapabilities.Level.DEFAULT));
    }
    if (!typeQuery.getSortKeys().isEmpty()
        && !getCapabilities().supportsQueryOp(Operation.SORTING)) {
      throw new IllegalArgumentException(
          String.format(
              "Feature providers with level %s do not support SORTING",
              getCapabilities().getLevel()));
    }
  }
}
