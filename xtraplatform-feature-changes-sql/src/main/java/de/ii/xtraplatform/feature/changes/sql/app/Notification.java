/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.changes.sql.app;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.features.domain.FeatureChange;
import de.ii.xtraplatform.features.domain.FeatureChange.Action;
import de.ii.xtraplatform.features.domain.ImmutableFeatureChange;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;
import org.threeten.extra.Interval;

@Value.Immutable
public interface Notification {

  Splitter SPLITTER = Splitter.on(",").trimResults();

  static Notification from(String featureType, String payload) {
    return ImmutableNotification.builder().featureType(featureType).payload(payload).build();
  }

  String getFeatureType();

  String getPayload();

  @Value.Lazy
  default FeatureChange asFeatureChange() {
    List<String> parameters = SPLITTER.splitToList(getPayload());

    if (parameters.size() < 8) {
      throw new IllegalArgumentException("incomplete parameters - " + getPayload());
    }

    Action action = Action.fromString(parameters.get(0));

    if (action == Action.UNKNOWN) {
      throw new IllegalArgumentException("unknown action - " + parameters.get(0));
    }

    return ImmutableFeatureChange.builder()
        .action(action)
        .featureType(getFeatureType())
        .featureIds(parseFeatureId(parameters.get(1)))
        .newInterval(parseInterval(parameters.subList(2, 4)))
        .newBoundingBox(parseBbox(parameters.subList(4, 8)))
        .build();
  }

  private static List<String> parseFeatureId(String featureId) {
    if (featureId.isEmpty() || "NULL".equalsIgnoreCase(featureId)) {
      return ImmutableList.of();
    }

    return ImmutableList.of(featureId);
  }

  private static Optional<Interval> parseInterval(List<String> interval) {
    if (interval.get(1).isEmpty()) {
      // an instant
      return parseTimestamp(interval.get(0)).map(instant -> Interval.of(instant, instant));
    } else {
      // an interval
      Optional<Instant> begin = parseTimestamp(interval.get(0));
      Optional<Instant> end = parseTimestamp(interval.get(1));
      if (begin.isPresent() && end.isPresent()) {
        return Optional.of(Interval.of(begin.get(), end.get()));
      }
    }
    return Optional.empty();
  }

  private static Optional<Instant> parseTimestamp(String timestamp) {
    try {
      return Optional.of(Instant.parse(timestamp));
    } catch (DateTimeException e) {
      return Optional.empty();
    }
  }

  private static Optional<BoundingBox> parseBbox(List<String> bbox) {
    try {
      return Optional.of(
          BoundingBox.of(
              Double.parseDouble(bbox.get(0)),
              Double.parseDouble(bbox.get(1)),
              Double.parseDouble(bbox.get(2)),
              Double.parseDouble(bbox.get(3)),
              OgcCrs.CRS84));
    } catch (NumberFormatException | IndexOutOfBoundsException e) {
      return Optional.empty();
    }
  }
}
