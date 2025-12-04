/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTileset3dFiles.Builder.class)
public interface Tileset3dFiles extends Tileset3dCommon, Buildable<Tileset3dFiles> {

  /**
   * @langEn Relative path of the root `tileset.json` file in the store under `resources/3dtiles`.
   * @langDe Relativer Pfad der Root-`tileset.json`-Datei im Store unter `resources/3dtiles`.
   * @default null
   * @since v4.6
   */
  String getSource();

  @Override
  default ImmutableTileset3dFiles.Builder getBuilder() {
    return new ImmutableTileset3dFiles.Builder().from(this);
  }

  abstract class Builder implements BuildableBuilder<Tileset3dFiles> {}

  default Tileset3dFiles mergeDefaults(Tileset3dFilesDefaults defaults) {
    ImmutableTileset3dFiles.Builder withDefaults = getBuilder();

    return withDefaults.build();
  }
}
