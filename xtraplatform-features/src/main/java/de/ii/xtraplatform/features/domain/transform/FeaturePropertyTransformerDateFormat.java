/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.features.domain.SchemaBase;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public interface FeaturePropertyTransformerDateFormat extends FeaturePropertyValueTransformer {

  Logger LOGGER = LoggerFactory.getLogger(FeaturePropertyTransformerDateFormat.class);

  String TYPE = "DATE_FORMAT";
  ZoneId UTC = ZoneId.of("UTC");
  String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZZZZZ";
  String DATE_FORMAT = "yyyy-MM-dd";
  // built once; rebuilding per value (and parseBest's throwing query fallbacks) showed up in
  // profiling
  DateTimeFormatter PARSER = DateTimeFormatter.ofPattern("yyyy-MM-dd[['T'][' ']HH:mm:ss][.SSS][X]");

  @Override
  default String getType() {
    return TYPE;
  }

  @Override
  default List<SchemaBase.Type> getSupportedPropertyTypes() {
    return ImmutableList.of(SchemaBase.Type.DATETIME);
  }

  ZoneId getDefaultTimeZone();

  // cached per transformer instance instead of rebuilt for every value
  @Value.Lazy
  default DateTimeFormatter formatter() {
    return DateTimeFormatter.ofPattern(getParameter());
  }

  @Override
  default String transform(String currentPropertyPath, String input) {
    // TODO: variable fractions
    try {
      ZonedDateTime zdt = parse(input, getDefaultTimeZone());

      return formatter().format(zdt);
    } catch (Throwable e) {
      LOGGER.warn(
          "{} transformation for property '{}' with value '{}' failed: {}",
          getType(),
          getPropertyPath(),
          input,
          e.getMessage());
    }

    return input;
  }

  static ZonedDateTime parse(String input, ZoneId defaultTimeZone) {
    // Parse once and branch on the resolved fields. parseBest instead tries OffsetDateTime/
    // LocalDateTime/LocalDate via queries that throw — filling in a stack trace — for every value
    // without an offset, which is costly when applied per feature.
    TemporalAccessor ta = PARSER.parse(input);

    if (ta.isSupported(ChronoField.OFFSET_SECONDS)) {
      return OffsetDateTime.from(ta).atZoneSameInstant(UTC);
    }
    if (ta.isSupported(ChronoField.HOUR_OF_DAY)) {
      return LocalDateTime.from(ta).atZone(defaultTimeZone).withZoneSameInstant(UTC);
    }
    return LocalDate.from(ta).atStartOfDay(UTC);
  }
}
