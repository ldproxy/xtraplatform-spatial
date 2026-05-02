/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;
import org.threeten.extra.Interval;

@Value.Immutable
public interface FeatureChange {

  enum Action {
    CREATE,
    UPDATE,
    DELETE,
    UNKNOWN;

    public static Action fromString(String action) {
      return "create".equalsIgnoreCase(action) || "insert".equalsIgnoreCase(action)
          ? CREATE
          : "update".equalsIgnoreCase(action)
              ? UPDATE
              : "delete".equalsIgnoreCase(action) ? DELETE : UNKNOWN;
    }
  }

  Action getAction();

  String getFeatureType();

  List<String> getFeatureIds();

  Optional<BoundingBox> getOldBoundingBox();

  Optional<BoundingBox> getNewBoundingBox();

  Optional<Interval> getOldInterval();

  Optional<Interval> getNewInterval();

  @Value.Derived
  default Instant getModified() {
    return Instant.now().truncatedTo(ChronoUnit.SECONDS);
  }
}
