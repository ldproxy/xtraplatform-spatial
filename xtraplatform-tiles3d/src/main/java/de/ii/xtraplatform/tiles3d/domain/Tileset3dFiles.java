/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.tiles.domain.ImmutableMinMax;
import de.ii.xtraplatform.tiles.domain.MinMax;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTileset3dFiles.Builder.class)
public interface Tileset3dFiles extends Tileset3dCommon, Buildable<Tileset3dFiles> {

  @DocIgnore
  @Override
  BuildableMap<MinMax, ImmutableMinMax.Builder> getLevels();

  @DocIgnore
  @Override
  Optional<LonLat> getCenter();

  /**
   * @langEn Relative path of the root `tileset.json` file in the store under `resources/3dtiles`.
   * @langDe Relativer Pfad der Root-`tileset.json`-Datei im Store unter `resources/3dtiles`.
   * @default null
   * @since v4.6
   */
  String getSource();

  /**
   * @langEn Tile Matrix Set of the tiles in the MBTiles file.
   * @langDe Kachelschema der Kacheln in der MBTiles-Datei.
   * @default WebMercatorQuad
   * @since v4.0
   */
  @Nullable
  String getTileMatrixSet();

  @Value.Check
  default void checkSingleTileMatrixSet() {
    Preconditions.checkState(
        getLevels().size() <= 1,
        "There must be no more than one tile matrix set associated with an MBTiles file. Found: %s.",
        getLevels().size());
  }

  @Override
  default ImmutableTileset3dFiles.Builder getBuilder() {
    return new ImmutableTileset3dFiles.Builder().from(this);
  }

  abstract class Builder implements BuildableBuilder<Tileset3dFiles> {}

  default Tileset3dFiles mergeDefaults(Tileset3dFilesDefaults defaults) {
    ImmutableTileset3dFiles.Builder withDefaults = getBuilder();

    if (this.getLevels().isEmpty()) {
      withDefaults.levels(defaults.getLevels());
    }
    if (this.getCenter().isEmpty() && defaults.getCenter().isPresent()) {
      withDefaults.center(defaults.getCenter());
    }

    if (Objects.isNull(this.getTileMatrixSet())) {
      withDefaults.tileMatrixSet(defaults.getTileMatrixSet());
    }

    return withDefaults.build();
  }
}
