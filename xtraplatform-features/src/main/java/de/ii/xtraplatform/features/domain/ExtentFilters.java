/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.cql.domain.Bbox;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.Interval;
import de.ii.xtraplatform.cql.domain.Operand;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.SIntersects;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.cql.domain.TIntersects;
import de.ii.xtraplatform.cql.domain.Temporal;
import de.ii.xtraplatform.cql.domain.TemporalLiteral;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.features.domain.MultiFeatureQuery.SubQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Filters that restrict a query to a fixed extent. Intersects is used instead of contains so that
 * features and tiles select the same features, even if some of them reach beyond the extent.
 */
public final class ExtentFilters {

  private ExtentFilters() {}

  public static List<Cql2Expression> of(
      FeatureSchema type,
      Optional<BoundingBox> spatialExtent,
      Optional<org.threeten.extra.Interval> temporalExtent) {
    List<Cql2Expression> filters = new ArrayList<>();

    spatialExtent
        .flatMap(extent -> type.getFilterGeometry().map(geometry -> spatial(geometry, extent)))
        .ifPresent(filters::add);

    temporalExtent
        .flatMap(extent -> primaryTemporal(type).map(temporal -> temporal(temporal, extent)))
        .ifPresent(filters::add);

    return filters;
  }

  /**
   * Adds the extent filters of each queried type to a query. Queries for a single feature are not
   * restricted, so every feature stays reachable by its id.
   */
  public static Query restrict(Query query, Function<String, List<Cql2Expression>> filtersForType) {
    if (query instanceof FeatureQuery featureQuery && !featureQuery.returnsSingleFeature()) {
      return ImmutableFeatureQuery.builder()
          .from(featureQuery)
          .addAllFilters(filtersForType.apply(featureQuery.getType()))
          .build();
    }
    if (query instanceof MultiFeatureQuery multiQuery) {
      return ImmutableMultiFeatureQuery.builder()
          .from(multiQuery)
          .queries(
              multiQuery.getQueries().stream()
                  .map(subQuery -> restrictSubQuery(subQuery, filtersForType))
                  .toList())
          .build();
    }
    return query;
  }

  private static SubQuery restrictSubQuery(
      SubQuery subQuery, Function<String, List<Cql2Expression>> filtersForType) {
    return ImmutableSubQuery.builder()
        .from(subQuery)
        .addAllFilters(filtersForType.apply(subQuery.getType()))
        .build();
  }

  private static Cql2Expression spatial(FeatureSchema geometry, BoundingBox extent) {
    return SIntersects.of(
        Property.of(geometry.getFullPathAsString()), SpatialLiteral.of(Bbox.of(extent)));
  }

  private static Cql2Expression temporal(Temporal property, org.threeten.extra.Interval extent) {
    return TIntersects.of(property, new TemporalLiteral.Builder(extent).build());
  }

  private static Optional<Temporal> primaryTemporal(FeatureSchema type) {
    return type.getPrimaryInstant()
        .<Temporal>map(instant -> Property.of(instant.getFullPathAsString()))
        .or(
            () ->
                type.getPrimaryInterval()
                    .map(
                        interval ->
                            Interval.of(
                                List.of(boundOf(interval.first()), boundOf(interval.second())))));
  }

  private static Operand boundOf(FeatureSchema property) {
    return property == null
        ? TemporalLiteral.of("..")
        : Property.of(property.getFullPathAsString());
  }
}
