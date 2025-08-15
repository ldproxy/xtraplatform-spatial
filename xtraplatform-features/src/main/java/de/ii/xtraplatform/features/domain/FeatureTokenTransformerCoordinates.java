/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformation;
import de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformer;
import de.ii.xtraplatform.geometries.domain.transform.ImmutableCrsTransform;
import de.ii.xtraplatform.geometries.domain.transform.ImmutableSimplifyLine;
import java.util.Optional;

public class FeatureTokenTransformerCoordinates extends FeatureTokenTransformer {

  private final Optional<CrsTransformer> crsTransformerTargetCrs;
  private final Optional<CrsTransformer> crsTransformerWgs84;

  public FeatureTokenTransformerCoordinates(
      Optional<CrsTransformer> crsTransformerTargetCrs,
      Optional<CrsTransformer> crsTransformerWgs84) {
    this.crsTransformerTargetCrs = crsTransformerTargetCrs;
    this.crsTransformerWgs84 = crsTransformerWgs84;
  }

  @Override
  public void onGeometry(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    Geometry<?> geometry = context.geometry();
    if (geometry != null) {
      CoordinatesTransformation next = null;

      // A SECONDARY_GEOMETRY is always forced to WGS84 longitude/latitude, not the target CRS
      boolean isSecondaryGeometry =
          context.schema().filter(SchemaBase::isSecondaryGeometry).isPresent();

      // since the secondary geometry is in WGS84, the offset may be in the wrong unit, so we skip
      // simplification
      if (context.query().getMaxAllowableOffset() > 0 && !isSecondaryGeometry) {
        next = ImmutableSimplifyLine.of(Optional.empty(), context.query().getMaxAllowableOffset());
      }

      Optional<CrsTransformer> crsTransformer =
          isSecondaryGeometry ? crsTransformerWgs84 : crsTransformerTargetCrs;
      if (crsTransformer.isPresent()) {
        next = ImmutableCrsTransform.of(Optional.ofNullable(next), crsTransformer.get());
      }

      if (next != null) {
        geometry = geometry.accept(new CoordinatesTransformer(next));
        context.setGeometry(geometry);
      }
    }

    getDownstream().onGeometry(context);
  }
}
