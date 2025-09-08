/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.ImmutableMutationResult.Builder;
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import de.ii.xtraplatform.features.domain.Tuple;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTokenStatsCollector extends FeatureTokenTransformerSql {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureTokenStatsCollector.class);

  private final Builder builder;
  private final EpsgCrs crs;
  private int axis = 0;
  private int dim = 2;
  private String xmin = "";
  private String ymin = "";
  private String xmax = "";
  private String ymax = "";
  private String start = "";
  private String end = "";

  public FeatureTokenStatsCollector(Builder builder, EpsgCrs crs) {
    this.builder = builder;
    this.crs = crs;
  }

  @Override
  public void onStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    // TODO: get crs
    this.dim = context.geometryDimension().orElse(2);

    super.onStart(context);
  }

  @Override
  public void onEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (!xmin.isEmpty() && !ymin.isEmpty() && !xmax.isEmpty() && !ymax.isEmpty()) {
      builder.spatialExtent(
          BoundingBox.of(
              Double.parseDouble(xmin),
              Double.parseDouble(ymin),
              Double.parseDouble(xmax),
              Double.parseDouble(ymax),
              crs));
    }

    if (!start.isEmpty() || !end.isEmpty()) {
      builder.temporalExtent(Tuple.of(parseTemporal(start), parseTemporal(end)));
    }

    super.onEnd(context);
  }

  private Long parseTemporal(String temporal) {
    if (temporal.isEmpty()) {
      return null;
    }
    try {
      if (temporal.length() > 10) {
        return ZonedDateTime.parse(temporal).toInstant().toEpochMilli();
      }
      return LocalDate.parse(temporal).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
    } catch (Throwable e) {
      return null;
    }
  }

  @Override
  public void onValue(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (Objects.nonNull(context.value())) {
      String value = context.value();

      if (context.inGeometry()) {
        if (axis == 0 && (xmin.isEmpty() || value.compareTo(xmin) < 0)) {
          this.xmin = value;
        }
        if (axis == 0 && (xmax.isEmpty() || value.compareTo(xmax) > 0)) {
          this.xmax = value;
        }
        if (axis == 1 && (ymin.isEmpty() || value.compareTo(ymin) < 0)) {
          this.ymin = value;
        }
        if (axis == 1 && (ymax.isEmpty() || value.compareTo(ymax) > 0)) {
          this.ymax = value;
        }

        this.axis = (axis + 1) % dim;
      } else if (hasRole(context, Role.PRIMARY_INSTANT)) {
        if (start.isEmpty() || value.compareTo(start) < 0) {
          this.start = value;
        }
        if (end.isEmpty() || value.compareTo(end) > 0) {
          this.end = value;
        }
      } else if (hasRole(context, Role.PRIMARY_INTERVAL_START)) {
        if (start.isEmpty() || value.compareTo(start) < 0) {
          this.start = value;
        }
      } else if (hasRole(context, Role.PRIMARY_INTERVAL_END)) {
        if (end.isEmpty() || value.compareTo(end) > 0) {
          this.end = value;
        }
      }
    }

    super.onValue(context);
  }

  private boolean hasRole(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context, Role role) {
    return context
        .mapping()
        .getSchemaForRole(role)
        .filter(schema -> Objects.equals(schema.getFullPathAsString(), context.pathAsString()))
        .isPresent();
  }
}
