/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import de.ii.xtraplatform.docs.DocFile;
import de.ii.xtraplatform.docs.DocStep;
import de.ii.xtraplatform.docs.DocStep.Step;
import de.ii.xtraplatform.docs.DocTable;
import de.ii.xtraplatform.docs.DocTable.ColumnSet;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.features.domain.ProviderData;
import java.util.Map;

/**
 * @langEn # 3D Tiles
 *     <p>There are currently two types of 3D Tiles providers:
 *     <p><code>
 * - [Features](10-features.md): The tiles are derived from a feature provider.
 * - [Files](20-files.md): The tiles are retrieved from the store.
 *     </code>
 *     <p>## Configuration
 *     <p>These are common configuration options for all provider types.
 *     <p>{@docTable:cfgProperties}
 * @langDe # Tiles
 *     <p>Es werden aktuell zwei Arten von 3D Tile-Providern unterstützt:
 *     <p><code>
 * - [Features](10-features.md): Die Kacheln werden aus einem Feature-Provider abgeleitet.
 * - [Files](20-files.md): Die Kacheln liegen im Store vor.
 *     </code>
 *     <p>## Konfiguration
 *     <p>Dies sind gemeinsame Konfigurations-Optionen für alle Provider-Typen.
 *     <p>{@docTable:cfgProperties}
 * @ref:cfgProperties {@link de.ii.xtraplatform.tiles3d.domain.ImmutableTile3dProviderCommonData}
 */
@DocFile(
    path = "providers/tile3d",
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
public interface Tile3dProviderData extends ProviderData {

  String ENTITY_TYPE = "providers";
  String PROVIDER_TYPE = "3DTILE";

  /**
   * @langEn Always `3DTILE`.
   * @langDe Immer `3DTILE`.
   */
  @Override
  String getProviderType();

  /**
   * @langEn `FEATURES` or `FILES`.
   * @langDe `FEATURES` oder `FILES`.
   */
  @Override
  String getProviderSubType();

  /**
   * @langEn Defaults for all `tilesets`.
   * @langDe Defaults für alle `tilesets`.
   * @since v4.6
   */
  Tileset3dCommonDefaults getTilesetDefaults();

  /**
   * @langEn Definition of tilesets.
   * @langDe Definition von Tilesets.
   * @since v4.6
   * @default {}
   */
  Map<String, ? extends Tileset3dCommon> getTilesets();

  abstract class Builder<T extends Tile3dProviderData.Builder<T>>
      implements EntityDataBuilder<Tile3dProviderData> {

    public abstract T id(String id);

    public abstract T providerType(String providerType);

    public abstract T providerSubType(String providerSubType);
  }
}
