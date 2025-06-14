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

  @Override
  default String getType() {
    return TYPE;
  }

  @Override
  default List<SchemaBase.Type> getSupportedPropertyTypes() {
    return ImmutableList.of(SchemaBase.Type.DATETIME);
  }

  ZoneId getDefaultTimeZone();

  @Override
  default String transform(String currentPropertyPath, String input) {
    // TODO: variable fractions
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(getParameter());

      ZonedDateTime zdt = parse(input, getDefaultTimeZone());

      return formatter.format(zdt);
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
    DateTimeFormatter parser =
        DateTimeFormatter.ofPattern("yyyy-MM-dd[['T'][' ']HH:mm:ss][.SSS][X]");

    TemporalAccessor ta =
        parser.parseBest(input, OffsetDateTime::from, LocalDateTime::from, LocalDate::from);

    if (ta instanceof OffsetDateTime) {
      return ((OffsetDateTime) ta).atZoneSameInstant(UTC);
    } else if (ta instanceof LocalDateTime) {
      return ((LocalDateTime) ta).atZone(defaultTimeZone).withZoneSameInstant(UTC);
    } else if (ta instanceof LocalDate) {
      return ((LocalDate) ta).atStartOfDay(UTC);
    }

    throw new IllegalArgumentException(
        String.format("Input '%s' could not be parsed as a date/time value", input));
  }
}
