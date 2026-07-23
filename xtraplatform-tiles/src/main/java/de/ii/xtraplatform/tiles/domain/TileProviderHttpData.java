/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocFile;
import de.ii.xtraplatform.docs.DocStep;
import de.ii.xtraplatform.docs.DocStep.Step;
import de.ii.xtraplatform.docs.DocTable;
import de.ii.xtraplatform.docs.DocTable.ColumnSet;
import de.ii.xtraplatform.docs.DocVar;
import de.ii.xtraplatform.entities.domain.EntityDataDefaults;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * # HTTP
 *
 * @langEn With this tile provider, the tiles are obtained via HTTP, e.g. from a
 *     [TileServer-GL](https://github.com/maptiler/tileserver-gl) instance.
 *     <p>## Configuration
 *     <p>{@docTable:properties}
 *     <p>{@docVar:tilesetDefaults}
 *     <p>{@docTable:tilesetDefaults}
 *     <p>{@docVar:tileset}
 *     <p>{@docTable:tileset}
 *     <p>## Example
 *     <p>{@docVar:examples}
 * @langDe Bei diesem Tile-Provider werden die Kacheln über HTTP bezogen, z.B. von einer
 *     [TileServer-GL](https://github.com/maptiler/tileserver-gl) Instanz.
 *     <p>## Konfiguration
 *     <p>{@docTable:properties}
 *     <p>{@docVar:tilesetDefaults}
 *     <p>{@docTable:tilesetDefaults}
 *     <p>{@docVar:tileset}
 *     <p>{@docTable:tileset}
 *     <p>## Beispiel
 *     <p>{@docVar:examples}
 * @ref:cfgProperties {@link de.ii.xtraplatform.tiles.domain.ImmutableTileProviderHttpData}
 * @ref:tilesetDefaults {@link de.ii.xtraplatform.tiles.domain.TilesetHttpDefaults}
 * @ref:tilesetDefaultsTable {@link de.ii.xtraplatform.tiles.domain.ImmutableTilesetHttpDefaults}
 * @ref:tileset {@link de.ii.xtraplatform.tiles.domain.TilesetHttp}
 * @ref:tilesetTable {@link de.ii.xtraplatform.tiles.domain.ImmutableTilesetHttp}
 * @examplesAll <code>
 * ```yaml
 * id: earthatnight-tiles
 * providerType: TILE
 * providerSubType: HTTP
 * tilesets:
 *   __all__:
 *     id: __all__
 *     urlTemplate: https://demo.ldproxy.net/earthatnight/tiles/{{tileMatrixSet}}/{{tileMatrix}}/{{tileRow}}/{{tileCol}}?f={{fileExtension}}
 *     levels:
 *       WebMercatorQuad:
 *         min: 0
 *         max: 6
 *     encodings:
 *       JPEG: jpeg
 * ```
 * </code>
 */
@DocFile(
    path = "providers/tile",
    name = "60-http.md",
    tables = {
      @DocTable(
          name = "properties",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:cfgProperties}"),
            @DocStep(type = Step.JSON_PROPERTIES)
          },
          columnSet = ColumnSet.JSON_PROPERTIES),
      @DocTable(
          name = "tilesetDefaults",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:tilesetDefaultsTable}"),
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
          name = "tilesetDefaults",
          value = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:tilesetDefaults}"),
            @DocStep(type = Step.TAG, params = "{@bodyBlock}")
          }),
      @DocVar(
          name = "tileset",
          value = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:tileset}"),
            @DocStep(type = Step.TAG, params = "{@bodyBlock}")
          }),
      @DocVar(
          name = "examples",
          value = {
            // @DocStep(type = Step.TAG_REFS, params = "{@ref:cfg}"),
            @DocStep(type = Step.TAG, params = "{@examples}")
          }),
    })
@Value.Immutable
@JsonDeserialize(builder = ImmutableTileProviderHttpData.Builder.class)
public interface TileProviderHttpData extends TileProviderData {

  String PROVIDER_SUBTYPE = "HTTP";
  String ENTITY_SUBTYPE =
      String.format("%s/%s", PROVIDER_TYPE, PROVIDER_SUBTYPE).toLowerCase(Locale.ROOT);

  /**
   * @langEn Always `HTTP`.
   * @langDe Immer `HTTP`.
   */
  @Override
  String getProviderSubType();

  /**
   * @langEn Defaults for all `tilesets`, see [Tileset Defaults](#tileset-defaults).
   * @langDe Defaults für alle `tilesets`, siehe [Tileset Defaults](#tileset-defaults).
   * @since v3.4
   */
  @Nullable
  @Override
  TilesetHttpDefaults getTilesetDefaults();

  /**
   * @langEn Definition of tilesets, see [Tileset](#tileset).
   * @langDe Definition von Tilesets, see [Tileset](#tileset).
   * @since v3.4
   * @default {}
   */
  @Override
  BuildableMap<TilesetHttp, ImmutableTilesetHttp.Builder> getTilesets();

  abstract class Builder extends TileProviderData.Builder<ImmutableTileProviderHttpData.Builder> {
    @Override
    public ImmutableTileProviderHttpData.Builder fillRequiredFieldsWithPlaceholders() {
      return this.id(EntityDataDefaults.PLACEHOLDER)
          .providerType(EntityDataDefaults.PLACEHOLDER)
          .providerSubType(EntityDataDefaults.PLACEHOLDER);
    }

    public abstract Map<String, ImmutableTilesetHttp.Builder> getTilesets();

    public abstract ImmutableTileProviderHttpData.Builder tilesetDefaultsBuilder(
        ImmutableTilesetHttpDefaults.Builder tilesetDefaults);
  }
}
