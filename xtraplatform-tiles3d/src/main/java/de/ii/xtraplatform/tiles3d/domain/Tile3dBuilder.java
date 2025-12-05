/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.github.azahnen.dagger.annotations.AutoMultiBind;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.geometries.domain.Polygon;
import java.util.Optional;

@AutoMultiBind
public interface Tile3dBuilder {

  int getPriority();

  byte[] generateTile(
      Tile3dCoordinates tile3dCoordinates,
      Tileset3dFeatures tileset,
      BoundingBox boundingBox,
      Optional<Polygon> exclusionPolygon,
      FeatureProvider featureProvider,
      String apiId,
      String collectionId);
}
