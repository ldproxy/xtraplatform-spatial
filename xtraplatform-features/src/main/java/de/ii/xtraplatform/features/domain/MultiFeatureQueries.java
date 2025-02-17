/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

public interface MultiFeatureQueries {

  String CAPABILITY = "multiQueries";
  String LABEL = "Execute multi-queries";

  default boolean supportsCql2() {
    return false;
  }

  default boolean supportsAccenti() {
    return false;
  }

  default FeatureStream getFeatureStream(MultiFeatureQuery query) {
    throw new UnsupportedOperationException();
  }
}
