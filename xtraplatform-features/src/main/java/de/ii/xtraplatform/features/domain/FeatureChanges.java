/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

public interface FeatureChanges {

  String CAPABILITY = "changes";
  String LABEL = "Detect dataset changes";

  void addListener(DatasetChangeListener listener);

  void addListener(FeatureChangeListener listener);

  void removeListener(DatasetChangeListener listener);

  void removeListener(FeatureChangeListener listener);

  void handle(DatasetChange change);

  void handle(FeatureChange change);
}
