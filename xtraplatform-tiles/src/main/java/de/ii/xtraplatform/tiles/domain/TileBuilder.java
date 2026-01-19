/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.github.azahnen.dagger.annotations.AutoMultiBind;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import java.util.Optional;
import java.util.Set;

@AutoMultiBind
public interface TileBuilder {

  // buffer in relation to the "formal" tile size (256 for WMQ)
  int BUFFER_SIZE_FORMAL = 8;

  int getPriority();

  boolean isApplicable(String featureProviderId);

  byte[] getMvtData(
      TileQuery tileQuery,
      TilesetFeatures tileset,
      Set<FeatureSchema> types,
      EpsgCrs nativeCrs,
      BoundingBox tileBounds,
      Optional<BoundingBox> clippedBounds,
      FeatureProvider featureProvider,
      PropertyTransformations baseTransformations);
}
