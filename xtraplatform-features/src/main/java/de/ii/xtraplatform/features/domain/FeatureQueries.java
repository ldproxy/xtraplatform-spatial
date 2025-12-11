/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.List;

public interface FeatureQueries {

  String CAPABILITY = "queries";

  FeatureProviderCapabilities getCapabilities();

  long getFeatureCount(String typeName);

  // TODO: replace with capabilities
  default boolean supportsCql2() {
    return false;
  }

  // TODO: replace with capabilities
  default boolean supportsAccenti() {
    return false;
  }

  // TODO: replace with capabilities
  default boolean supportsIsNull() {
    return false;
  }

  default boolean skipUnusedPipelineSteps() {
    return false;
  }

  default FeatureStream getFeatureStream(FeatureQuery query) {
    throw new UnsupportedOperationException();
  }

  // TODO: move to FeatureProvider2 or FeatureMetadata
  FeatureSchema getQueryablesSchema(
      FeatureSchema schema,
      List<String> included,
      List<String> excluded,
      String pathSeparator,
      boolean cleanupKeys);

  // TODO: move to FeatureProvider2 or FeatureMetadata
  FeatureSchema getSortablesSchema(
      FeatureSchema schema, List<String> included, List<String> excluded, String pathSeparator);
}
