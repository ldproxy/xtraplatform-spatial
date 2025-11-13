/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.tiles.domain.ImmutableMinMax.Builder;
import de.ii.xtraplatform.tiles.domain.MinMax;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTileset3dFilesDefaults.Builder.class)
public interface Tileset3dFilesDefaults extends Tileset3dCommonDefaults {

  @DocIgnore
  @Override
  Optional<LonLat> getCenter();

  @DocIgnore
  @Override
  BuildableMap<MinMax, Builder> getLevels();

  /**
   * @langEn Tile Matrix Set of the tiles in the MBTiles file.
   * @langDe Kachelschema der Kacheln in der MBTiles-Datei.
   * @default WebMercatorQuad
   * @since v4.0
   */
  @Value.Default
  default String getTileMatrixSet() {
    return "WebMercatorQuad";
  }
}
