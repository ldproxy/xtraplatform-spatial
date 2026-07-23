/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.legacy.TargetMapping;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

/**
 * @author zahnen
 */
public interface FeatureTransformer {
  String getTargetFormat();

  void onStart(OptionalLong numberReturned, OptionalLong numberMatched) throws IOException;

  void onEnd() throws IOException;

  void onFeatureStart(TargetMapping mapping) throws IOException;

  void onFeatureEnd() throws IOException;

  void onPropertyStart(TargetMapping mapping, List<Integer> multiplicities) throws IOException;

  void onPropertyText(String text) throws IOException;

  void onPropertyEnd() throws IOException;

  void onGeometryStart(TargetMapping mapping, GeometryType type, Integer dimension)
      throws IOException;

  void onGeometryNestedStart() throws IOException;

  void onGeometryCoordinates(String text) throws IOException;

  void onGeometryNestedEnd() throws IOException;

  void onGeometryEnd() throws IOException;
}
