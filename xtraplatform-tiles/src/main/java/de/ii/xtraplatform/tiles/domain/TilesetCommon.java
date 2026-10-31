/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.tiles.domain.ImmutableMinMax.Builder;
import java.util.Optional;

public interface TilesetCommon extends TilesetCommonDefaults {
  /**
   * @langEn The tileset id.
   * @langDe Die Tileset-Id.
   * @since v3.4
   */
  String getId();

  @Override
  BuildableMap<MinMax, Builder> getLevels();

  @Override
  Optional<LonLat> getCenter();
}
