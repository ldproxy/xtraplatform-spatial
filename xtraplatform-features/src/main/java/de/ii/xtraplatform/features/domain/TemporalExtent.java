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
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.threeten.extra.Interval;

/**
 * Temporal extent. Bounds are either dates (yyyy-MM-dd, interpreted in UTC) or date-times with
 * offset (yyyy-MM-ddTHH:mm:ss[.SSS]Z or ±HH:mm). A missing bound is open.
 */
@Value.Immutable
@JsonDeserialize(builder = ImmutableTemporalExtent.Builder.class)
public interface TemporalExtent {

  @Nullable
  String getStart();

  @Nullable
  String getEnd();

  @Nullable
  Boolean getComputed();

  @JsonIgnore
  default Optional<Interval> toInterval() {
    if (getStart() == null && getEnd() == null) {
      return Optional.empty();
    }

    Instant start = getStart() == null ? Instant.MIN : parseStart(getStart());
    Instant end = getEnd() == null ? Instant.MAX : parseEnd(getEnd());

    return Optional.of(Interval.of(start, end));
  }

  @Value.Check
  default void check() {
    boolean hasBounds = getStart() != null || getEnd() != null;

    Preconditions.checkState(
        !(hasBounds && Boolean.TRUE.equals(getComputed())),
        "TemporalExtent: 'computed' and explicit start/end must not be set at the same time.");

    try {
      toInterval();
    } catch (DateTimeParseException e) {
      throw new IllegalStateException(
          String.format(
              "TemporalExtent: '%s' is neither a date (yyyy-MM-dd) nor a date-time with offset.",
              e.getParsedString()),
          e);
    } catch (DateTimeException e) {
      throw new IllegalStateException("TemporalExtent: start must not be after end.", e);
    }
  }

  private static Instant parseStart(String value) {
    return isDate(value)
        ? LocalDate.parse(value).atStartOfDay().toInstant(ZoneOffset.UTC)
        : OffsetDateTime.parse(value).toInstant();
  }

  private static Instant parseEnd(String value) {
    return isDate(value)
        ? LocalDate.parse(value).atTime(LocalTime.of(23, 59, 59)).toInstant(ZoneOffset.UTC)
        : OffsetDateTime.parse(value).toInstant();
  }

  private static boolean isDate(String value) {
    return !value.contains("T");
  }
}
