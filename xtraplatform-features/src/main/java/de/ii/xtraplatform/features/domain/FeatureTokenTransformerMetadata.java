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
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

public class FeatureTokenTransformerMetadata extends FeatureTokenTransformer {

  private final Consumer<Instant> lastModifiedSetter;
  private final Consumer<BoundingBox> spatialExtentSetter;
  private final Consumer<Tuple<Instant, Instant>> temporalExtentSetter;
  private Optional<EpsgCrs> crs;
  private double[][] minMax = null;
  private String start = "";
  private String end = "";
  private boolean isSingleFeature = false;
  private String lastModified = "";

  public FeatureTokenTransformerMetadata(ImmutableResult.Builder resultBuilder) {
    this.lastModifiedSetter = resultBuilder::lastModified;
    this.spatialExtentSetter = resultBuilder::spatialExtent;
    this.temporalExtentSetter = resultBuilder::temporalExtent;
  }

  public <X> FeatureTokenTransformerMetadata(ImmutableResultReduced.Builder<X> resultBuilder) {
    this.lastModifiedSetter = resultBuilder::lastModified;
    this.spatialExtentSetter = resultBuilder::spatialExtent;
    this.temporalExtentSetter = resultBuilder::temporalExtent;
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
        temporalExtentSetter.accept(Tuple.of(Instant.parse(start), Instant.parse(end)));
      } else if (!start.isEmpty()) {
        temporalExtentSetter.accept(Tuple.of(Instant.parse(start), null));
      } else if (!end.isEmpty()) {
        temporalExtentSetter.accept(Tuple.of(null, Instant.parse(end)));
      }
    } catch (Throwable ignore) {
    }

    try {
      if (!lastModified.isEmpty()) {
        lastModifiedSetter.accept(Instant.parse(lastModified));
      }
    } catch (Throwable ignore) {
    }

    super.onEnd(context);
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
