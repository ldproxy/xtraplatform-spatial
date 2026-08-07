/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.app;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zahnen
 */
public class GmlMultiplicityTracker {

  private static final Joiner JOINER = Joiner.on('.');

  private final Map<String, Integer> currentMultiplicities;

  public GmlMultiplicityTracker() {
    this.currentMultiplicities = new HashMap<>();
  }

  public void reset() {
    currentMultiplicities.clear();
  }

  // TODO multilevel, mark multiple properties in mapping and pass multi-elements to constructor,
  // see SqlMultiplicityMapping
  public void track(List<String> path) {
    if (!path.isEmpty()) {
      String pathId = JOINER.join(path);
      currentMultiplicities.put(pathId, currentMultiplicities.getOrDefault(pathId, 0) + 1);
    }
  }

  public void trackEnd(List<String> path) {}

  public List<Integer> getMultiplicitiesForPath(List<String> path) {

    boolean hasMultiple = false;
    List<Integer> multiplicities = new ArrayList<>();

    for (int i = 0; i < path.size(); i++) {
      String pathId = JOINER.join(path.subList(0, i));

      if (currentMultiplicities.getOrDefault(pathId, 1) > 1) {
        hasMultiple = true;
        multiplicities.add(currentMultiplicities.get(pathId));
      }
    }

    /*if (hasMultiple) {
        LOGGER.debug(Joiner.on(' ').join(multiplicities));
    }*/

    return hasMultiple ? multiplicities : ImmutableList.of(1);
  }
}
