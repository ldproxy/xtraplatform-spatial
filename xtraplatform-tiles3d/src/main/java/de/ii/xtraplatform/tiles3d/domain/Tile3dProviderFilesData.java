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
import de.ii.xtraplatform.docs.DocFile;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.docs.DocStep;
import de.ii.xtraplatform.docs.DocStep.Step;
import de.ii.xtraplatform.docs.DocTable;
import de.ii.xtraplatform.docs.DocTable.ColumnSet;
import de.ii.xtraplatform.docs.DocVar;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.entities.domain.EntityDataDefaults;
import java.util.Map;
import org.immutables.value.Value;

/**
 * # MBTiles
 *
 * @langEn With this tile provider, the tiles are provided via an [MBTiles
 *     file](https://github.com/mapbox/mbtiles-spec). The tile format and all other properties of
 *     the tileset are derived from the contents of the MBTiles file. Only the `WebMercatorQuad`
 *     tiling scheme is supported.
 *     <p>## Configuration
 *     <p>{@docTable:properties}
 *     <p>### Tileset
 *     <p>{@docTable:tileset}
 *     <p>## Example
 *     <p>{@docVar:examples}
 * @langDe Bei diesem Tile-Provider werden die Kacheln über eine
 *     [MBTiles-Datei](https://github.com/mapbox/mbtiles-spec) bereitgestellt. Das Kachelformat und
 *     alle anderen Eigenschaften des Tileset ergeben sich aus dem Inhalt der MBTiles-Datei.
 *     Unterstützt wird nur das Kachelschema `WebMercatorQuad`.
 *     <p>## Konfiguration
 *     <p>{@docTable:properties}
 *     <p>### Tileset
 *     <p>{@docTable:tileset}
 *     <p>## Beispiel
 *     <p>{@docVar:examples}
 * @ref:cfgProperties {@link de.ii.xtraplatform.tiles3d.domain.ImmutableTile3dProviderFilesData}
 * @ref:tilesetTable {@link de.ii.xtraplatform.tiles3d.domain.ImmutableTileset3dFiles}
 * @examplesAll <code>
 * ```yaml
 * id: zoomstack-tiles
 * providerType: TILE
 * providerSubType: MBTILES
 * tilesets:
 *   __all__:
 *     id: __all__
 *     source: zoomstack/OS_Open_Zoomstack.mbtiles
 * ```
 * </code>
 */
@DocFile(
    path = "providers/tile",
    name = "30-mbtiles.md",
    tables = {
      @DocTable(
          name = "properties",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:cfgProperties}"),
            @DocStep(type = Step.JSON_PROPERTIES)
          },
          columnSet = ColumnSet.JSON_PROPERTIES),
      @DocTable(
          name = "tileset",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:tilesetTable}"),
            @DocStep(type = Step.JSON_PROPERTIES)
          },
          columnSet = ColumnSet.JSON_PROPERTIES),
    },
    vars = {
      @DocVar(
          name = "examples",
          value = {
            // @DocStep(type = Step.TAG_REFS, params = "{@ref:cfg}"),
            @DocStep(type = Step.TAG, params = "{@examples}")
          }),
    })
@Value.Immutable
@JsonDeserialize(builder = ImmutableTile3dProviderFilesData.Builder.class)
public interface Tile3dProviderFilesData extends Tile3dProviderData {

  String PROVIDER_SUBTYPE = "FILES";
  String ENTITY_SUBTYPE = String.format("%s/%s", PROVIDER_TYPE, PROVIDER_SUBTYPE).toLowerCase();

  /**
   * @langEn Always `FILES`.
   * @langDe Immer `FILES`.
   */
  @Override
  String getProviderSubType();

  @DocIgnore
  @Value.Default
  @Override
  // note: ImmutableTilesetMbTilesDefaults is used, because using the interface results in an error
  default ImmutableTileset3dFilesDefaults getTilesetDefaults() {
    return new ImmutableTileset3dFilesDefaults.Builder().build();
  }

  @Override
  Map<String, Tileset3dFiles> getTilesets();

  abstract class Builder
      extends Tile3dProviderData.Builder<ImmutableTile3dProviderFilesData.Builder>
      implements EntityDataBuilder<Tile3dProviderData> {
    @Override
    public ImmutableTile3dProviderFilesData.Builder fillRequiredFieldsWithPlaceholders() {
      return this.id(EntityDataDefaults.PLACEHOLDER)
          .providerType(EntityDataDefaults.PLACEHOLDER)
          .providerSubType(EntityDataDefaults.PLACEHOLDER);
    }
  }

  @Value.Check
  default void checkSingleTileset() {
    Preconditions.checkState(
        getTilesets().size() <= 1,
        "There can only be one tileset in an MBTiles provider. Found: %s.",
        getTilesets().size());
  }
}
