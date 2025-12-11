/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocFile;
import de.ii.xtraplatform.docs.DocStep;
import de.ii.xtraplatform.docs.DocStep.Step;
import de.ii.xtraplatform.docs.DocTable;
import de.ii.xtraplatform.docs.DocTable.ColumnSet;
import de.ii.xtraplatform.docs.DocVar;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.entities.domain.EntityDataDefaults;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * # Features
 *
 * @langEn In this 3D tile provider, the [3D Tiles
 *     1.1](https://docs.ogc.org/cs/22-025r4/22-025r4.html) in glTF format are derived from a
 *     [Feature Provider](../feature/README.md). The tilesets use implicit quadtree tiling, subtrees
 *     are encoded using the binary format for compactness.
 *     <p>The only [refinement strategy](https://docs.ogc.org/cs/22-025r4/22-025r4.html#toc19) that
 *     * is supported is `ADD`. Use the `contentFilters` configuration option to specify at which *
 *     level of the tile hierarchy a building will be represented. Each building should be included
 *     * on exactly one level.
 *     <p>## Limitations
 *     <p>The provider currently depends on the building block [Features -
 *     glTF](features_-_gltf.md#limitations) and inherits its limitations.
 *     <p>In addition, the following information in Subtrees is not supported: property tables, tile
 *     metadata, content metadata, and subtree metadata.
 *     <p>## Configuration
 *     <p>{@docTable:properties}
 *     <p>{@docVar:tilesetDefaults}
 *     <p>{@docTable:tilesetDefaults}
 *     <p>{@docVar:tileset}
 *     <p>{@docTable:tileset}
 *     <p>{@docVar:seeding}
 *     <p>{@docTable:seeding}
 *     <p>## Storage
 *     <p>The files of the tilesets are stored in the resource store in the * directory
 *     `3dtiles/{apiId}/cache_dyn/{tileset}/`.
 *     <p>## Example
 *     <p>{@docVar:examples}
 * @langDe Bei diesem 3D Tile-Provider werden die [3D Tiles
 *     1.1](https://docs.ogc.org/cs/22-025r4/22-025r4.html) im Format glTF aus einem [Feature
 *     Provider](../feature/README.md) abgeleitet. Die Tilesets verwenden implizite
 *     Quadtree-Kachelung, Subtrees werden aus Gründen der Kompaktheit im Binärformat kodiert.
 *     <p>Die einzige unterstützte
 *     [Refinement-Strategie](https://docs.ogc.org/cs/22-025r4/22-025r4.html#toc19) ist `ADD`.
 *     Verwenden Sie die Konfigurationsoption `contentFilters`, um anzugeben, auf welcher Ebene der
 *     Kachel-Hierarchie ein Gebäude dargestellt werden soll. Jedes Gebäude sollte genau auf einer
 *     Ebene enthalten sein.
 *     <p>## Limitierungen
 *     <p>Der Provider hat aktuell noch eine Abhängigkeit auf den Baustein [Features -
 *     glTF](features_-_gltf.md#limitierungen) und erbt dessen Limitierungen.
 *     <p>Darüber hinaus werden die folgenden Informationen in Subtrees nicht unterstützt:
 *     Eigenschaftstabellen (Property Tables), Kachel-Metadaten (Tile Metadata), Inhalts-Metadaten
 *     (Content Metadata) und Metadaten von Subtrees.
 *     <p>## Konfiguration
 *     <p>{@docTable:properties}
 *     <p>{@docVar:tilesetDefaults}
 *     <p>{@docTable:tilesetDefaults}
 *     <p>{@docVar:tileset}
 *     <p>{@docTable:tileset}
 *     <p>{@docVar:seeding}
 *     <p>{@docTable:seeding}
 *     <p>## Speicherung
 *     <p>Die Dateien der Tilesets werden im Ressourcen-Store im Verzeichnis
 *     `3dtiles/{apiId}/cache_dyn/{tileset}/` abgelegt.
 *     <p>## Beispiel
 *     <p>{@docVar:examples}
 * @ref:cfgProperties {@link de.ii.xtraplatform.tiles3d.domain.ImmutableTile3dProviderFeaturesData}
 * @ref:tilesetDefaults {@link de.ii.xtraplatform.tiles3d.domain.Tileset3dFeaturesDefaults}
 * @ref:tilesetDefaultsTable {@link
 *     de.ii.xtraplatform.tiles3d.domain.ImmutableTileset3dFeaturesDefaults}
 * @ref:tileset {@link de.ii.xtraplatform.tiles3d.domain.Tileset3dFeatures}
 * @ref:tilesetTable {@link de.ii.xtraplatform.tiles3d.domain.ImmutableTileset3dFeatures}
 * @ref:seeding {@link de.ii.xtraplatform.tiles3d.domain.SeedingOptions3d}
 * @ref:seedingTable {@link de.ii.xtraplatform.tiles3d.domain.ImmutableSeedingOptions3d}
 * @examplesAll <code>
 * ```yaml
 * id: cologne_lod2-3dtiles
 * providerType: 3DTILE
 * providerSubType: FEATURES
 * seeding:
 *   runOnStartup: true
 *   purge: true
 *   jobSize: S
 * tilesetDefaults:
 *   featureProvider: cologne_lod2
 *   clampToEllipsoid: true
 * tilesets:
 *   building:
 *     id: building
 *     featureType: building
 *     geometricErrorRoot: 4096.0
 *     subtreeLevels: 3
 *     contentLevels:
 *       min: 2
 *       max: 2
 * ```
 * </code>
 */
@DocFile(
    path = "providers/tile3d",
    name = "10-features.md",
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
      @DocTable(
          name = "seeding",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:seedingTable}"),
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
          name = "seeding",
          value = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:seeding}"),
            @DocStep(type = Step.TAG, params = "{@bodyBlock}")
          }),
      @DocVar(
          name = "examples",
          value = {@DocStep(type = Step.TAG, params = "{@examples}")}),
    })
@Value.Immutable
@JsonDeserialize(builder = ImmutableTile3dProviderFeaturesData.Builder.class)
public interface Tile3dProviderFeaturesData extends Tile3dProviderData {

  String PROVIDER_SUBTYPE = "FEATURES";
  String ENTITY_SUBTYPE = String.format("%s/%s", PROVIDER_TYPE, PROVIDER_SUBTYPE).toLowerCase();

  /**
   * @langEn Always `FEATURES`.
   * @langDe Immer `FEATURES`.
   */
  @Override
  String getProviderSubType();

  @Nullable
  @Override
  Tileset3dFeaturesDefaults getTilesetDefaults();

  @Override
  BuildableMap<Tileset3dFeatures, ImmutableTileset3dFeatures.Builder> getTilesets();

  /**
   * @langEn Controls how and when tiles are precomputed, see [Seeding](#seeding).
   * @langDe Steuert wie und wann Kacheln vorberechnet werden, siehe [Seeding](#seeding).
   * @since v4.6
   * @default {}
   */
  Optional<SeedingOptions3d> getSeeding();

  abstract class Builder
      extends Tile3dProviderData.Builder<ImmutableTile3dProviderFeaturesData.Builder>
      implements EntityDataBuilder<Tile3dProviderData> {
    @Override
    public ImmutableTile3dProviderFeaturesData.Builder fillRequiredFieldsWithPlaceholders() {
      return this.id(EntityDataDefaults.PLACEHOLDER)
          .providerType(EntityDataDefaults.PLACEHOLDER)
          .providerSubType(EntityDataDefaults.PLACEHOLDER);
    }

    public abstract Map<String, ImmutableTileset3dFeatures.Builder> getTilesets();

    public abstract ImmutableTile3dProviderFeaturesData.Builder tilesetDefaultsBuilder(
        ImmutableTileset3dFeaturesDefaults.Builder tilesetDefaults);
  }
}
