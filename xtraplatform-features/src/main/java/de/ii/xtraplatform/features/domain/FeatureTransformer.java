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
import java.util.List;
import java.util.OptionalLong;

/**
 * @author zahnen
 */
@SuppressWarnings("PMD.SignatureDeclareThrowsException")
public interface FeatureTransformer {
  String getTargetFormat();

  void onStart(OptionalLong numberReturned, OptionalLong numberMatched) throws Exception;

  void onEnd() throws Exception;

  void onFeatureStart(TargetMapping mapping) throws Exception;

  void onFeatureEnd() throws Exception;

  void onPropertyStart(TargetMapping mapping, List<Integer> multiplicities) throws Exception;

  void onPropertyText(String text) throws Exception;

  void onPropertyEnd() throws Exception;

  void onGeometryStart(TargetMapping mapping, GeometryType type, Integer dimension)
      throws Exception;

  void onGeometryNestedStart() throws Exception;

  void onGeometryCoordinates(String text) throws Exception;

  void onGeometryNestedEnd() throws Exception;

  void onGeometryEnd() throws Exception;
}
