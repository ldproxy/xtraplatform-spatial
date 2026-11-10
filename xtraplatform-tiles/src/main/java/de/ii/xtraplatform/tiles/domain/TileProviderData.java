/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import de.ii.xtraplatform.docs.DocFile;
import de.ii.xtraplatform.docs.DocStep;
import de.ii.xtraplatform.docs.DocStep.Step;
import de.ii.xtraplatform.docs.DocTable;
import de.ii.xtraplatform.docs.DocTable.ColumnSet;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.features.domain.ProviderData;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.LinkedHashMap;
import java.util.Map;
import org.immutables.value.Value;

/**
 * @langEn # Tiles
 *     <p>There are currently three types of Tile providers:
 *     <p><code>
 * - [Features](10-features.md): The tiles are derived from a feature provider.
 * - [MbTiles](30-mbtiles.md): The tiles are retrieved from one or more MBTiles files.
 * - [HTTP](60-http.md): The tiles are retrieved via HTTP, e.g. from a TileServer GL instance.
 *     </code>
 *     <p>## Configuration
 *     <p>These are common configuration options for all provider types.
 *     <p>{@docTable:cfgProperties}
 * @langDe # Tiles
 *     <p>Es werden aktuell drei Arten von Tile-Providern unterstützt:
 *     <p><code>
 * - [Features](10-features.md): Die Kacheln werden aus einem Feature-Provider abgeleitet.
 * - [MbTiles](30-mbtiles.md): Die Kacheln liegen in einer oder mehreren MBTiles-Dateien vor.
 * - [HTTP](60-http.md): Die Kacheln werden via HTTP abgerufen, z.B. von einer TileServer GL Instanz.
 *     </code>
 *     <p>## Konfiguration
 *     <p>Dies sind gemeinsame Konfigurations-Optionen für alle Provider-Typen.
 *     <p>{@docTable:cfgProperties}
 * @ref:cfgProperties {@link de.ii.xtraplatform.tiles.domain.ImmutableTileProviderCommonData}
 */
@DocFile(
    path = "providers/tile",
    name = "README.md",
    tables = {
      @DocTable(
          name = "cfgProperties",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:cfgProperties}"),
            @DocStep(type = Step.JSON_PROPERTIES)
          },
          columnSet = ColumnSet.JSON_PROPERTIES),
    })
public interface TileProviderData extends ProviderData {

  String ENTITY_TYPE = "providers";
  String PROVIDER_TYPE = "TILE";

  /**
   * @langEn Always `TILE`.
   * @langDe Immer `TILE`.
   */
  @Override
  String getProviderType();

  /**
   * @langEn `FEATURES` or `MBILES` or `HTTP`.
   * @langDe `FEATURES` oder `MBILES` oder `HTTP`.
   */
  @Override
  String getProviderSubType();

  /**
   * @langEn Defaults for all `tilesets`.
   * @langDe Defaults für alle `tilesets`.
   * @since v3.4
   */
  TilesetCommonDefaults getTilesetDefaults();

  /**
   * @langEn Definition of tilesets.
   * @langDe Definition von Tilesets.
   * @since v3.4
   * @default {}
   */
  Map<String, ? extends TilesetCommon> getTilesets();

  @JsonIgnore
  @Value.Lazy
  default Map<String, Map<String, Range<Integer>>> getTmsRanges() {
    return getTilesets().entrySet().stream()
        .map(
            entry -> {
              Map<String, Range<Integer>> ranges =
                  new LinkedHashMap<>(getTilesetDefaults().getTmsRanges());
              ranges.putAll(entry.getValue().getTmsRanges());

              return new SimpleImmutableEntry<>(entry.getKey(), ranges);
            })
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  abstract class Builder<T extends TileProviderData.Builder<T>>
      implements EntityDataBuilder<TileProviderData> {

    public abstract T id(String id);

    public abstract T providerType(String providerType);

    public abstract T providerSubType(String providerSubType);
  }
}
