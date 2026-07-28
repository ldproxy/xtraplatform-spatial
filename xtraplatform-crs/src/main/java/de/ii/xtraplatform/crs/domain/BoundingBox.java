/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Splitter;
import de.ii.xtraplatform.crs.domain.ImmutableBoundingBox.Builder;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(builder = "new", deepImmutablesDetection = true)
@JsonDeserialize(builder = ImmutableBoundingBox.Builder.class)
public interface BoundingBox {

  static BoundingBox of(String bboxString, EpsgCrs crs) {
    List<Double> coords =
        Splitter.on(',')
            .trimResults()
            .splitToStream(bboxString)
            .map(Double::valueOf)
            .collect(Collectors.toUnmodifiableList());
    if (coords.size() == 4) {
      return of(coords.get(0), coords.get(1), coords.get(2), coords.get(3), crs);
    } else if (coords.size() == 6) {
      return of(
          coords.get(0),
          coords.get(1),
          coords.get(2),
          coords.get(3),
          coords.get(4),
          coords.get(5),
          crs);
    }

    throw new IllegalArgumentException(String.format("Invalid bbox: %s", bboxString));
  }

  static BoundingBox of(double xmin, double ymin, double xmax, double ymax, EpsgCrs crs) {
    return new ImmutableBoundingBox.Builder()
        .xmin(xmin)
        .ymin(ymin)
        .xmax(xmax)
        .ymax(ymax)
        .epsgCrs(crs)
        .build();
  }

  static BoundingBox of(
      double xmin, double ymin, double zmin, double xmax, double ymax, double zmax, EpsgCrs crs) {
    return new ImmutableBoundingBox.Builder()
        .xmin(xmin)
        .ymin(ymin)
        .zmin(zmin)
        .xmax(xmax)
        .ymax(ymax)
        .zmax(zmax)
        .epsgCrs(crs)
        .build();
  }

  static BoundingBox merge(BoundingBox first, BoundingBox second) {
    if (!Objects.equals(first.getEpsgCrs(), second.getEpsgCrs())) {
      return first;
    }

    Builder builder =
        new ImmutableBoundingBox.Builder()
            .xmin(Math.min(first.getXmin(), second.getXmin()))
            .ymin(Math.min(first.getYmin(), second.getYmin()))
            .xmax(Math.max(first.getXmax(), second.getXmax()))
            .ymax(Math.max(first.getYmax(), second.getYmax()))
            .epsgCrs(first.getEpsgCrs());

    if (first.is3d() && second.is3d()) {
      builder
          .zmin(Math.min(first.getZmin(), second.getZmin()))
          .zmax(Math.max(first.getZmax(), second.getZmax()));
    }

    return builder.build();
  }

  static Optional<BoundingBox> intersect2d(BoundingBox first, BoundingBox second, double buffer) {
    if (!Objects.equals(first.getEpsgCrs(), second.getEpsgCrs())) {
      return Optional.of(first);
    }

    BoundingBox bbox =
        new Builder()
            .xmin(Math.max(first.getXmin(), second.getXmin() - buffer))
            .ymin(Math.max(first.getYmin(), second.getYmin() - buffer))
            .xmax(Math.min(first.getXmax(), second.getXmax() + buffer))
            .ymax(Math.min(first.getYmax(), second.getYmax() + buffer))
            .epsgCrs(first.getEpsgCrs())
            .build();

    if (bbox.getXmin() > bbox.getXmax() || bbox.getYmin() > bbox.getYmax()) {
      return Optional.empty();
    }

    return Optional.of(bbox);
  }

  static boolean intersects(BoundingBox first, BoundingBox second) {
    return Objects.equals(first.getEpsgCrs(), second.getEpsgCrs())
        && Math.max(first.getXmin(), second.getXmin())
            <= Math.min(first.getXmax(), second.getXmax())
        && Math.max(first.getYmin(), second.getYmin())
            <= Math.min(first.getYmax(), second.getYmax())
        && (!first.is3d()
            || Math.max(first.getZmin(), second.getZmin())
                <= Math.min(first.getZmax(), second.getZmax()));
  }

  double getXmin();

  double getYmin();

  @Nullable
  Double getZmin();

  double getXmax();

  double getYmax();

  @Nullable
  Double getZmax();

  @Value.Default
  default EpsgCrs getEpsgCrs() {
    return OgcCrs.CRS84;
  }

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean is3d() {
    return Objects.nonNull(getZmin()) && Objects.nonNull(getZmax());
  }

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default double[] toArray() {
    return is3d()
        ? new double[] {getXmin(), getYmin(), getZmin(), getXmax(), getYmax(), getZmax()}
        : new double[] {getXmin(), getYmin(), getXmax(), getYmax()};
  }
}
