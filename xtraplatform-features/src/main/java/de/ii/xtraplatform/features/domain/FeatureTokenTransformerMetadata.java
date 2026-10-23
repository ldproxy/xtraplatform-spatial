/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.geometries.domain.transform.MinMaxDeriver;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTokenTransformerMetadata extends FeatureTokenTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FeatureTokenTransformerMetadata.class);

  // Accepts the timestamp forms that the feature providers deliver: a date, a date and time with
  // 'T' or a space as separator, an optional fraction of a second with any number of digits, and an
  // optional time-zone offset in any of the ISO forms ('Z', '+HH', '+HHmm', '+HH:MM').
  private static final DateTimeFormatter FLEXIBLE_PARSER =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .optionalStart()
          .optionalStart()
          .appendLiteral('T')
          .optionalEnd()
          .optionalStart()
          .appendLiteral(' ')
          .optionalEnd()
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .optionalStart()
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .optionalEnd()
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .optionalEnd()
          .optionalEnd()
          .optionalStart()
          .appendOffsetId()
          .optionalEnd()
          .optionalStart()
          .appendOffset("+HHmm", "Z")
          .optionalEnd()
          .optionalStart()
          .appendOffset("+HH", "Z")
          .optionalEnd()
          .toFormatter();

  private final Consumer<Instant> lastModifiedSetter;
  private final Consumer<BoundingBox> spatialExtentSetter;
  private final Consumer<Tuple<Instant, Instant>> temporalExtentSetter;
  // the time zone of the provider, applied to values without a time zone
  private final ZoneId defaultTimeZone;
  private Optional<EpsgCrs> crs;
  private double[][] minMax = null;
  private String start = "";
  private String end = "";
  private boolean isSingleFeature = false;
  private String lastModified = "";

  public FeatureTokenTransformerMetadata(
      ImmutableResult.Builder resultBuilder, ZoneId defaultTimeZone) {
    this.lastModifiedSetter = resultBuilder::lastModified;
    this.spatialExtentSetter = resultBuilder::spatialExtent;
    this.temporalExtentSetter = resultBuilder::temporalExtent;
    this.defaultTimeZone = defaultTimeZone;
  }

  public <X> FeatureTokenTransformerMetadata(
      ImmutableResultReduced.Builder<X> resultBuilder, ZoneId defaultTimeZone) {
    this.lastModifiedSetter = resultBuilder::lastModified;
    this.spatialExtentSetter = resultBuilder::spatialExtent;
    this.temporalExtentSetter = resultBuilder::temporalExtent;
    this.defaultTimeZone = defaultTimeZone;
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.crs = context.query().getCrs();
    this.isSingleFeature = context.metadata().isSingleFeature();

    super.onStart(context);
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    try {
      if (minMax != null) {
        spatialExtentSetter.accept(
            minMax[0].length == 2
                ? BoundingBox.of(
                    minMax[0][0],
                    minMax[0][1],
                    minMax[1][0],
                    minMax[1][1],
                    crs.orElse(OgcCrs.CRS84))
                : BoundingBox.of(
                    minMax[0][0],
                    minMax[0][1],
                    minMax[0][2],
                    minMax[1][0],
                    minMax[1][1],
                    minMax[1][2],
                    crs.orElse(OgcCrs.CRS84h)));
      }
    } catch (Throwable ignore) {
    }

    try {
      if (!start.isEmpty() && !end.isEmpty()) {
        temporalExtentSetter.accept(Tuple.of(parseTemporal(start), parseTemporal(end)));
      } else if (!start.isEmpty()) {
        temporalExtentSetter.accept(Tuple.of(parseTemporal(start), null));
      } else if (!end.isEmpty()) {
        temporalExtentSetter.accept(Tuple.of(null, parseTemporal(end)));
      }
    } catch (Throwable ignore) {
    }

    if (!lastModified.isEmpty()) {
      try {
        // the value may be without a time zone, if it has not been normalized by a DATE_FORMAT
        // transformation (which is applied to every DATETIME property that has no other
        // transformation)
        lastModifiedSetter.accept(parseTemporal(lastModified));
      } catch (Throwable e) {
        // the last modification time is used for conditional requests, so a value that cannot be
        // parsed must not be ignored silently
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Could not parse the last modification time '{}' of the feature, the value is ignored. Reason: {}",
              lastModified,
              e.getMessage());
        }
      }
    }

    super.onEnd(context);
  }

  // The primary instant/interval properties may be DATETIME or DATE; a date is interpreted as
  // start of day. A value without a time zone is interpreted in the time zone of the provider
  // ("nativeTimeZone", UTC unless configured otherwise).
  private Instant parseTemporal(String value) {
    TemporalAccessor ta =
        FLEXIBLE_PARSER.parseBest(
            value, OffsetDateTime::from, LocalDateTime::from, LocalDate::from);
    if (ta instanceof OffsetDateTime) {
      return ((OffsetDateTime) ta).toInstant();
    } else if (ta instanceof LocalDateTime) {
      return ((LocalDateTime) ta).atZone(defaultTimeZone).toInstant();
    }
    return ((LocalDate) ta).atStartOfDay(defaultTimeZone).toInstant();
  }

  @Override
  public void onGeometry(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (context.schema().filter(SchemaBase::isPrimaryGeometry).isPresent()
        && Objects.nonNull(context.geometry())) {
      double[][] minMax2 = null;
      minMax2 = context.geometry().accept(new MinMaxDeriver());
      if (minMax == null) {
        minMax = minMax2;
      } else {
        for (int i = 0; i < minMax[0].length; i++) {
          if (minMax2[0][i] < minMax[0][i]) {
            minMax[0][i] = minMax2[0][i];
          }
          if (minMax2[1][i] > minMax[1][i]) {
            minMax[1][i] = minMax2[1][i];
          }
        }
      }
    }

    super.onGeometry(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (Objects.nonNull(context.value())) {
      String value = context.value();

      if (context.schema().filter(SchemaBase::isPrimaryInstant).isPresent()) {
        if (start.isEmpty() || value.compareTo(start) < 0) {
          this.start = value;
        }
        if (end.isEmpty() || value.compareTo(end) > 0) {
          this.end = value;
        }
      } else if (context.schema().filter(SchemaBase::isPrimaryIntervalStart).isPresent()) {
        if (start.isEmpty() || value.compareTo(start) < 0) {
          this.start = value;
        }
      } else if (context.schema().filter(SchemaBase::isPrimaryIntervalEnd).isPresent()) {
        if (end.isEmpty() || value.compareTo(end) > 0) {
          this.end = value;
        }
      }

      if (isSingleFeature && context.schema().map(SchemaBase::lastModified).orElse(false)) {
        this.lastModified = value;
      }
    }

    super.onValue(context);
  }
}
