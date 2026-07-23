/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformation;
import de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformer;
import de.ii.xtraplatform.geometries.domain.transform.ImmutableCrsTransform;
import de.ii.xtraplatform.geometries.domain.transform.ImmutableSimplifyLine;
import java.util.Optional;

public class FeatureTokenTransformerCoordinates extends FeatureTokenTransformer {

  private final Optional<CrsTransformer> crsTransformerTargetCrs;
  private final Optional<CrsTransformer> crsTransformerWgs84;
  private final CrsTransformerFactory crsTransformerFactory;

  public FeatureTokenTransformerCoordinates(
      Optional<CrsTransformer> crsTransformerTargetCrs,
      Optional<CrsTransformer> crsTransformerWgs84,
      CrsTransformerFactory crsTransformerFactory) {
    this.crsTransformerTargetCrs = crsTransformerTargetCrs;
    this.crsTransformerWgs84 = crsTransformerWgs84;
    this.crsTransformerFactory = crsTransformerFactory;
  }

  @Override
  public void onGeometry(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    Geometry<?> geometry = context.geometry();
    if (geometry != null) {
      // A geometry property that is stored in its own CRS (schema option `nativeCrs`) carries a
      // position as-is in a non-native CRS — possibly 1D/3D or a CRS the query pipeline cannot
      // transform. It is not transformed to the target CRS; when the property declares an
      // `originalCrs` (the CRS of the recorded positions, e.g. the authority axis order of a
      // geographic CRS whose stored coordinates follow the GIS axis order), the position is
      // transformed back to it, so downstream formats reproduce the recorded position verbatim.
      Optional<EpsgCrs> propertyCrs = context.schema().flatMap(SchemaBase::getNativeCrs);
      if (propertyCrs.isPresent()) {
        Optional<EpsgCrs> originalCrs = context.schema().flatMap(FeatureSchema::getOriginalCrs);
        if (originalCrs.isPresent() && !originalCrs.get().equals(propertyCrs.get())) {
          Optional<CrsTransformer> toOriginal =
              crsTransformerFactory.getTransformer(propertyCrs.get(), originalCrs.get());
          if (toOriginal.isPresent()) {
            context.setGeometry(
                geometry.accept(
                    new CoordinatesTransformer(
                        ImmutableCrsTransform.of(Optional.empty(), toOriginal.get()))));
          }
        }
        getDownstream().onGeometry(context);
        return;
      }

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
