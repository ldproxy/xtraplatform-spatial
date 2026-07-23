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
import java.util.Map;
import java.util.OptionalLong;

/**
 * @author zahnen
 */
public interface FeatureReaderGeneric
    extends FeatureReader<Map<String, String>, Map<String, String>> {

  @Override
  void onStart(OptionalLong numberReturned, OptionalLong numberMatched, Map<String, String> context)
      throws Exception;

  @Override
  void onEnd() throws Exception;

  @Override
  void onFeatureStart(List<String> path, Map<String, String> context) throws Exception;

  @Override
  void onFeatureEnd(List<String> path) throws Exception;

  @Override
  void onObjectStart(List<String> path, Map<String, String> context) throws Exception;

  @Override
  void onObjectEnd(List<String> path, Map<String, String> context) throws Exception;

  @Override
  void onArrayStart(List<String> path, Map<String, String> context) throws Exception;

  @Override
  void onArrayEnd(List<String> path, Map<String, String> context) throws Exception;

  @Override
  void onGeometry(List<String> path, Geometry<?> geometry, Map<String, String> context)
      throws Exception;

  @Override
  void onValue(List<String> path, String value, Map<String, String> context) throws Exception;
}
