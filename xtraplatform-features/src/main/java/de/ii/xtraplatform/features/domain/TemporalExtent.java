/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** Temporal extent with UTC ISO-8601 instants (yyyy-MM-ddTHH:mm:ss.SSSZ). */
@Value.Immutable
@JsonDeserialize(builder = ImmutableTemporalExtent.Builder.class)
public interface TemporalExtent {

  /**
   * Start of the temporal extent as UTC ISO-8601 instant (yyyy-MM-ddTHH:mm:ss[.SSS]Z), or {@code
   * null} for open start.
   */
  @Nullable
  String getStart();

  /**
   * End of the temporal extent as UTC ISO-8601 instant (yyyy-MM-ddTHH:mm:ss[.SSS]Z), or {@code
   * null} for open end.
   */
  @Nullable
  String getEnd();

  @Nullable
  Boolean getComputed();

  @Value.Derived
  @JsonIgnore
  default Instant getStartInstant() {
    return getStart() == null
        ? null
        : Instant.from(DateTimeFormatter.ISO_INSTANT.parse(getStart()));
  }

  @Value.Derived
  @JsonIgnore
  default Instant getEndInstant() {
    return getEnd() == null ? null : Instant.from(DateTimeFormatter.ISO_INSTANT.parse(getEnd()));
  }

  @Value.Check
  default void checkExclusiveComputed() {
    boolean hasBounds = getStart() != null || getEnd() != null;
    boolean autoCompute = Boolean.TRUE.equals(getComputed());

    Preconditions.checkState(
        !(hasBounds && autoCompute),
        "TemporalExtent: 'computed' and explicit start/end must not be set at the same time.");

    if (getStart() != null) {
      validateIsoInstant(getStart(), "start");
    }
    if (getEnd() != null) {
      validateIsoInstant(getEnd(), "end");
    }
  }

  private static void validateIsoInstant(String value, String field) {
    Preconditions.checkState(
        !value.matches("^-?\\d+$"),
        "TemporalExtent: '%s' must be a UTC ISO-8601 instant string, not a numeric timestamp.",
        field);
    try {
      DateTimeFormatter.ISO_INSTANT.parse(value);
    } catch (DateTimeParseException e) {
      Preconditions.checkState(
          false,
          "TemporalExtent: '%s' is not a valid UTC ISO-8601 instant (yyyy-MM-ddTHH:mm:ss.SSSZ): %s",
          field,
          value);
    }
  }
}
