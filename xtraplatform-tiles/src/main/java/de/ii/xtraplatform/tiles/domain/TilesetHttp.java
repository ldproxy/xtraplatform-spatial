/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.features.domain.SpatialExtent;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * @langEn ### Tileset
 *     <p>All options from [Tileset Defaults](#tileset-defaults) are also available and can be
 *     overriden here.
 * @langDe ### Tileset
 *     <p>Alle Optionen aus [Tileset Defaults](#tileset-defaults) sind ebenfalls verfübgar und
 *     können hier überschrieben werden.
 */
@Value.Immutable
@JsonDeserialize(builder = ImmutableTilesetHttp.Builder.class)
public interface TilesetHttp extends TilesetCommon, WithEncodings, Buildable<TilesetHttp> {

  @DocIgnore
  @Override
  BuildableMap<MinMax, ImmutableMinMax.Builder> getLevels();

  @DocIgnore
  @Override
  Optional<LonLat> getCenter();

  @DocIgnore
  @Override
  Map<TilesFormat, String> getEncodings();

  /**
   * @langEn Fixed spatial extent of the tileset in CRS84 (longitude/latitude). If not set, the
   *     extent of the tile matrix sets is used.
   * @langDe Fester räumlicher Extent des Tilesets in CRS84 (Länge/Breite). Wenn nicht gesetzt, wird
   *     der Extent der Kachelschemas verwendet.
   * @since v4.9
   */
  Optional<SpatialExtent> getExtent();

  /**
   * @langEn URL template for accessing tiles. Parameters to use are `{{tileMatrix}}`,
   *     `{{tileRow}}`, `{{tileCol}}` and `{{fileExtension}}`. Single curly braces are also allowed.
   * @langDe URL-Template für den Zugriff auf Kacheln. Zu verwenden sind die Parameter
   *     `{{tileMatrix}}`, `{{tileRow}}`, `{{tileCol}}` und `{{fileExtension}}`. Einfache
   *     geschweifte Klammern sind auch erlaubt.
   * @since v3.4
   */
  String getUrlTemplate();

  @Override
  default ImmutableTilesetHttp.Builder getBuilder() {
    return new ImmutableTilesetHttp.Builder().from(this);
  }

  abstract class Builder implements BuildableBuilder<TilesetHttp> {}
}
