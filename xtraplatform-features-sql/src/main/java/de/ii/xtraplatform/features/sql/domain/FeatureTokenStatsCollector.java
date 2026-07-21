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
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.transform.MinMaxDeriver;
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
  private Double xmin = null;
  private Double ymin = null;
  private Double xmax = null;
  private Double ymax = null;
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
    if (xmin != null && ymin != null && xmax != null && ymax != null) {
      builder.spatialExtent(BoundingBox.of(xmin, ymin, xmax, ymax, crs));
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

      if (hasRole(context, Role.PRIMARY_INSTANT)) {
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

  @Override
  public void onGeometry(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    // Only the primary geometry feeds the spatial extent: secondary geometry properties may
    // store positions in a different CRS (position variants), which must not be interpreted in
    // the native CRS. Mirrors the read side, where the extent is computed over the
    // filter/primary geometry column.
    if (Objects.nonNull(context.geometry()) && hasRole(context, Role.PRIMARY_GEOMETRY)) {
      Geometry<?> value = context.geometry();

      double[][] minMax = value.accept(new MinMaxDeriver());
      if (minMax.length > 1 && minMax[0].length >= dim && minMax[1].length >= dim) {
        this.xmin = xmin == null ? minMax[0][0] : Math.min(xmin, minMax[0][0]);
        this.xmax = xmax == null ? minMax[1][0] : Math.max(xmax, minMax[1][0]);
        this.ymin = ymin == null ? minMax[0][1] : Math.min(ymin, minMax[0][1]);
        this.ymax = ymax == null ? minMax[1][1] : Math.max(ymax, minMax[1][1]);
      }
    }

    super.onGeometry(context);
  }

  private boolean hasRole(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context, Role role) {
    return context
        .mapping()
        .getSchemaForRole(role)
        .filter(schema -> Objects.equals(schema.getFullPathAsString(), context.pathAsString()))
        .isPresent();
  }
}
