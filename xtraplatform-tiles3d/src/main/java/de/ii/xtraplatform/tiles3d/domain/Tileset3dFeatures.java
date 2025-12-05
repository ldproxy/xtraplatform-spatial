/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.tiles.domain.ImmutableTilesetFeatures;
import de.ii.xtraplatform.tiles.domain.MinMax;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * @langEn ### Tileset
 *     <p>All options from [Tileset Defaults](#tileset-defaults) are also available and can be
 *     overriden here.
 * @langDe ### Tileset
 *     <p>Alle Optionen aus [Tileset Defaults](#tileset-defaults) sind ebenfalls verfügbar und
 *     können hier überschrieben werden.
 */
@Value.Immutable
@JsonDeserialize(builder = ImmutableTilesetFeatures.Builder.class)
public interface Tileset3dFeatures
    extends Tileset3dCommon,
        Tile3dGenerationOptions,
        WithFeatureProvider,
        Buildable<Tileset3dFeatures> {
  String COMBINE_ALL = "*";

  @Override
  String getId();

  @DocIgnore
  @Override
  Optional<String> getFeatureProvider();

  /**
   * @langEn The name of the feature type. By default the tileset id is used.
   * @langDe Der Name des Feature-Types. Standardmäßig wird die Tileset-Id verwendet.
   * @default null
   * @since v4.6
   */
  Optional<String> getFeatureType();

  @DocIgnore
  @Nullable
  @Override
  Float getGeometricErrorRoot();

  @DocIgnore
  @Nullable
  @Override
  Boolean getClampToEllipsoid();

  @DocIgnore
  @Nullable
  @Override
  Integer getSubtreeLevels();

  @DocIgnore
  @Nullable
  @Override
  MinMax getContentLevels();

  @DocIgnore
  @Override
  List<String> getContentFilters();

  @DocIgnore
  @Override
  List<String> getTileFilters();

  /**
   * @langEn Instead of being generated using a `featureType`, a tileset may be composed of multiple
   *     other tilesets. Takes a list of tileset ids. A list with a single entry `*` combines all
   *     tilesets.
   * @langDe Anstatt aus einem `featureType` generiert zu werden, kann ein Tileset auch aus mehreren
   *     anderen Tilesets kombiniert werden. Der Wert ist eine Liste von Tileset-Ids oder eine Liste
   *     mit einem einzelnen Eintrag `*` um alle anderen Tilesets zu kombinieren.
   * @default []
   * @since v4.6
   */
  @DocIgnore
  // TODO: combine gltf tilesets to b3dm tileset?
  List<String> getCombine();

  @JsonIgnore
  @Value.Derived
  default boolean isCombined() {
    return !getCombine().isEmpty();
  }

  @Override
  default ImmutableTileset3dFeatures.Builder getBuilder() {
    return new ImmutableTileset3dFeatures.Builder().from(this);
  }

  abstract class Builder implements BuildableBuilder<Tileset3dFeatures> {}

  default Tileset3dFeatures mergeDefaults(Tileset3dFeaturesDefaults defaults) {
    if (Objects.isNull(defaults)) {
      return this;
    }

    ImmutableTileset3dFeatures.Builder withDefaults = getBuilder();

    if (this.getFeatureProvider().isEmpty() && defaults.getFeatureProvider().isPresent()) {
      withDefaults.featureProvider(defaults.getFeatureProvider());
    }
    if (Objects.isNull(this.getGeometricErrorRoot())
        && !Objects.isNull(defaults.getGeometricErrorRoot())) {
      withDefaults.geometricErrorRoot(defaults.getGeometricErrorRoot());
    }
    if (Objects.isNull(this.getClampToEllipsoid())
        && !Objects.isNull(defaults.getClampToEllipsoid())) {
      withDefaults.clampToEllipsoid(defaults.getClampToEllipsoid());
    }
    if (Objects.isNull(this.getSubtreeLevels()) && !Objects.isNull(defaults.getSubtreeLevels())) {
      withDefaults.subtreeLevels(defaults.getSubtreeLevels());
    }
    if (Objects.isNull(this.getContentLevels()) && !Objects.isNull(defaults.getContentLevels())) {
      withDefaults.contentLevels(defaults.getContentLevels());
    }
    if (this.getContentFilters().isEmpty() && !defaults.getContentFilters().isEmpty()) {
      withDefaults.contentFilters(defaults.getContentFilters());
    }

    return withDefaults.build();
  }
}
