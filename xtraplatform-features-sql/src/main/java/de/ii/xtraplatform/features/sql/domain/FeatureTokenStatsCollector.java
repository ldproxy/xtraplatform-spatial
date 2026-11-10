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
import de.ii.xtraplatform.features.domain.FeatureTransactions;
import de.ii.xtraplatform.features.domain.ImmutableMutationResult.Builder;
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.transform.MinMaxDeriver;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;

/**
 * Collects the spatial and temporal extent of the features of one token source into {@link Stats},
 * which can be shared by all sources of a mutation.
 */
public class FeatureTokenStatsCollector extends FeatureTokenTransformerSql {

  private final Stats stats;
  private final boolean partial;
  private int dim = 2;
  // the values of the current feature; null if not given, PATCH_NULL_VALUE if cleared
  private String featureInstant;
  private String featureStart;
  private String featureEnd;

  /**
   * @param stats the extents of the mutation
   * @param partial whether the features only carry the properties that change, so that a missing
   *     start or end leaves the extent as it is instead of making it unbounded
   */
  public FeatureTokenStatsCollector(Stats stats, boolean partial) {
    super();
    this.stats = stats;
    this.partial = partial;
  }

  @Override
  public void onStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    // NOPMD - TODO: get crs
    this.dim = context.geometryDimension().orElse(2);

    super.onStart(context);
  }

  @Override
  // null stands for a value that the next feature does not give
  @SuppressWarnings("PMD.NullAssignment")
  public void onFeatureStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    this.featureInstant = null;
    this.featureStart = null;
    this.featureEnd = null;

    super.onFeatureStart(context);
  }

  @Override
  public void onFeatureEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    SqlQueryMapping mapping = context.mapping();

    if (mapping.getSchemaForRole(Role.PRIMARY_INSTANT).isPresent()) {
      if (Objects.nonNull(featureInstant) && !isCleared(featureInstant)) {
        stats.addInstant(featureInstant);
      }
    } else if (mapping.getSchemaForRole(Role.PRIMARY_INTERVAL_START).isPresent()
        || mapping.getSchemaForRole(Role.PRIMARY_INTERVAL_END).isPresent()) {
      addInterval(mapping);
    }

    super.onFeatureEnd(context);
  }

  private void addInterval(SqlQueryMapping mapping) {
    // a type with only one of the two roles is unbounded on the other side
    if (mapping.getSchemaForRole(Role.PRIMARY_INTERVAL_START).isEmpty()) {
      stats.addIntervalStart(null);
    } else if (isDetermined(featureStart)) {
      stats.addIntervalStart(boundOrUnbounded(featureStart));
    }
    if (mapping.getSchemaForRole(Role.PRIMARY_INTERVAL_END).isEmpty()) {
      stats.addIntervalEnd(null);
    } else if (isDetermined(featureEnd)) {
      stats.addIntervalEnd(boundOrUnbounded(featureEnd));
    }
  }

  // a missing value leaves a partial update as it is, but is unbounded for a complete feature
  private boolean isDetermined(String value) {
    return !partial || Objects.nonNull(value);
  }

  private static String boundOrUnbounded(String value) {
    return Objects.isNull(value) || isCleared(value) ? null : value;
  }

  private static boolean isCleared(String value) {
    return Objects.equals(value, FeatureTransactions.PATCH_NULL_VALUE);
  }

  @Override
  public void onValue(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    String value = context.value();

    if (Objects.nonNull(value)) {
      if (hasRole(context, Role.PRIMARY_INSTANT)) {
        this.featureInstant = value;
      } else if (hasRole(context, Role.PRIMARY_INTERVAL_START)) {
        this.featureStart = value;
      } else if (hasRole(context, Role.PRIMARY_INTERVAL_END)) {
        this.featureEnd = value;
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
        stats.addBoundingBox(minMax[0][0], minMax[0][1], minMax[1][0], minMax[1][1]);
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

  /**
   * The extents of the features written by one mutation. In the temporal extent, null stands for an
   * unbounded start or end, as soon as one feature has no start or no end.
   */
  public static final class Stats {

    private final EpsgCrs crs;
    private Double xmin;
    private Double ymin;
    private Double xmax;
    private Double ymax;
    // earliest start and latest end that are set
    private String start;
    private String end;
    private boolean startUnbounded;
    private boolean endUnbounded;
    // whether a written feature determines the start or the end; not the case for a partial
    // update that leaves them as they are
    private boolean startKnown;
    private boolean endKnown;

    public Stats(EpsgCrs crs) {
      this.crs = crs;
    }

    void addBoundingBox(double minX, double minY, double maxX, double maxY) {
      this.xmin = xmin == null ? minX : Math.min(xmin, minX);
      this.ymin = ymin == null ? minY : Math.min(ymin, minY);
      this.xmax = xmax == null ? maxX : Math.max(xmax, maxX);
      this.ymax = ymax == null ? maxY : Math.max(ymax, maxY);
    }

    void addInstant(String value) {
      addIntervalStart(value);
      addIntervalEnd(value);
    }

    void addIntervalStart(String value) {
      this.startKnown = true;
      if (Objects.isNull(value)) {
        this.startUnbounded = true;
      } else if (Objects.isNull(start) || value.compareTo(start) < 0) {
        this.start = value;
      }
    }

    void addIntervalEnd(String value) {
      this.endKnown = true;
      if (Objects.isNull(value)) {
        this.endUnbounded = true;
      } else if (Objects.isNull(end) || value.compareTo(end) > 0) {
        this.end = value;
      }
    }

    /**
     * Applies values that the mutation sets for all of its features, e.g. the start and the open
     * end of new versions. A null value is an unbounded start or end.
     *
     * @param overrides the values per role
     */
    public void applyRoleOverrides(Map<Role, Object> overrides) {
      if (overrides.containsKey(Role.PRIMARY_INTERVAL_START)) {
        this.start = Objects.toString(overrides.get(Role.PRIMARY_INTERVAL_START), null);
        this.startUnbounded = Objects.isNull(start);
        this.startKnown = true;
      }
      if (overrides.containsKey(Role.PRIMARY_INTERVAL_END)) {
        this.end = Objects.toString(overrides.get(Role.PRIMARY_INTERVAL_END), null);
        this.endUnbounded = Objects.isNull(end);
        this.endKnown = true;
      }
    }

    /**
     * Sets the extents of the mutation result. A side that no written feature determines takes a
     * value of the other side: extents only grow, and the start of a feature is never after its
     * end, so that value does not change the extent on the unknown side. Without such a value, the
     * result has no temporal extent.
     *
     * @param builder the result of the mutation
     */
    public void writeTo(Builder builder) {
      if (xmin != null && ymin != null && xmax != null && ymax != null) {
        builder.spatialExtent(BoundingBox.of(xmin, ymin, xmax, ymax, crs));
      }

      writeTemporal(builder);
    }

    // the count comes from the conditional expressions of the two sides, splitting them up would
    // not make the method easier to follow
    @SuppressWarnings("PMD.NPathComplexity")
    private void writeTemporal(Builder builder) {
      String startValue = startKnown ? start : end;
      String endValue = endKnown ? end : start;
      boolean startOpen = startKnown && startUnbounded;
      boolean endOpen = endKnown && endUnbounded;

      if ((startOpen || Objects.nonNull(startValue)) && (endOpen || Objects.nonNull(endValue))) {
        builder.temporalExtent(
            Tuple.of(
                startOpen ? null : parseTemporal(startValue),
                endOpen ? null : parseTemporal(endValue)));
      }
    }

    private static Long parseTemporal(String temporal) {
      try {
        if (temporal.length() > 10) {
          return ZonedDateTime.parse(temporal).toInstant().toEpochMilli();
        }
        return LocalDate.parse(temporal).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
      } catch (DateTimeParseException e) {
        return null;
      }
    }
  }
}
