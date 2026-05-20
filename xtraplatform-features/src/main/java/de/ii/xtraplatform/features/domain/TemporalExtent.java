/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** Temporal extent with ISO-8601 dates (yyyy-MM-dd). */
@Value.Immutable
@JsonDeserialize(builder = ImmutableTemporalExtent.Builder.class)
public interface TemporalExtent {

  /** Start of the temporal extent as ISO-8601 date (yyyy-MM-dd), or {@code null} for open start. */
  @Nullable
  String getStart();

  /** End of the temporal extent as ISO-8601 date (yyyy-MM-dd), or {@code null} for open end. */
  @Nullable
  String getEnd();

  @Nullable
  Boolean getComputed();

  @Value.Check
  default void checkExclusiveComputed() {
    boolean hasBounds = getStart() != null || getEnd() != null;
    boolean hasComputed = getComputed() != null;

    Preconditions.checkState(
        !(hasBounds && hasComputed),
        "TemporalExtent: 'computed' and explicit start/end must not be set at the same time.");

    if (getStart() != null) {
      validateIsoDate(getStart(), "start");
    }
    if (getEnd() != null) {
      validateIsoDate(getEnd(), "end");
    }
  }

  private static void validateIsoDate(String value, String field) {
    Preconditions.checkState(
        !value.matches("^-?\\d+$"),
        "TemporalExtent: '%s' must be an ISO date string (yyyy-MM-dd), not a numeric timestamp.",
        field);
    Preconditions.checkState(
        !value.contains("T"),
        "TemporalExtent: '%s' must be an ISO date string (yyyy-MM-dd), timestamps are not allowed.",
        field);
    try {
      LocalDate.parse(value);
    } catch (DateTimeParseException e) {
      Preconditions.checkState(
          false,
          "TemporalExtent: '%s' is not a valid ISO-8601 date (yyyy-MM-dd): %s",
          field,
          value);
    }
  }
}
