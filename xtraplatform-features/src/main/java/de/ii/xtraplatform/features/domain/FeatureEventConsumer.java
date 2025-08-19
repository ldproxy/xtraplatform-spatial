/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.geometries.domain.Geometry;
import java.util.List;
import java.util.OptionalLong;

public interface FeatureEventConsumer {

  void onStart(OptionalLong numberReturned, OptionalLong numberMatched);

  void onEnd();

  void onFeatureStart();

  void onFeatureEnd();

  void onObjectStart(List<String> path);

  void onObjectEnd();

  void onArrayStart(List<String> path);

  void onArrayEnd();

  void onGeometry(List<String> path, Geometry<?> geometry);

  void onValue(List<String> path, String value, SchemaBase.Type valueType);
}
