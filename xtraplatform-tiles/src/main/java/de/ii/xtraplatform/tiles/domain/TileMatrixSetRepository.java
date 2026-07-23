/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.base.domain.resiliency.Volatile2;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Access to the cache for tile matrix set files. */
public interface TileMatrixSetRepository extends Volatile2 {

  List<String> PREDEFINED_TILE_MATRIX_SETS =
      ImmutableList.of(
          "WebMercatorQuad",
          "WorldCRS84Quad",
          "WorldMercatorWGS84Quad",
          "AdV_25832",
          "AdV_25833",
          "EU_25832",
          "gdi_de_25832");

  Optional<TileMatrixSet> get(String tileMatrixSetId);

  Map<String, TileMatrixSet> getAll();
}
