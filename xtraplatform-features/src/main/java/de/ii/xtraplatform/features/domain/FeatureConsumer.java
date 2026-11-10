/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * @author zahnen
 */
@SuppressWarnings("PMD.SignatureDeclareThrowsException")
public interface FeatureConsumer {
  void onStart(
      OptionalLong numberReturned, OptionalLong numberMatched, Map<String, String> additionalInfos)
      throws Exception;

  void onEnd() throws Exception;

  void onFeatureStart(List<String> path, Map<String, String> additionalInfos) throws Exception;

  void onFeatureEnd(List<String> path) throws Exception;

  void onPropertyStart(
      List<String> path, List<Integer> multiplicities, Map<String, String> additionalInfos)
      throws Exception;

  void onPropertyText(String text) throws Exception;

  void onPropertyEnd(List<String> path) throws Exception;
}
