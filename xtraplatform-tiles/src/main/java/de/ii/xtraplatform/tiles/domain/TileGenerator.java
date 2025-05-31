/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import de.ii.xtraplatform.base.domain.resiliency.VolatileComposed;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import java.util.Optional;
import javax.ws.rs.core.MediaType;

public interface TileGenerator extends ChainedTileProvider, VolatileComposed {

  String CAPABILITY = "generation";

  boolean supports(MediaType mediaType);

  byte[] generateTile(TileQuery tileQuery);

  // TODO: create on startup for all layers
  TileGenerationSchema getGenerationSchema(String tileset);

  Optional<BoundingBox> getBounds(String tilesetId);

  // TODO
  FeatureSchema getVectorSchema(String tilesetId, MediaType encoding);

  // TODO
  Optional<FeatureProvider> getFeatureProvider(String featureProviderId);
}
