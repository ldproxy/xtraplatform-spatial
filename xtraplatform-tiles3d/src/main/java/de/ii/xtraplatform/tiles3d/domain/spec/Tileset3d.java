/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain.spec;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.Funnel;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTileset3d.Builder.class)
public interface Tileset3d {

  default Tileset3d withUris(
      String uriPrefix, String implicitContentUri, String implicitSubtreeUri) {
    if (getRoot().getImplicitTiling().isEmpty()) {
      return withUris(uriPrefix);
    }

    return new ImmutableTileset3d.Builder()
        .from(this)
        .root(
            new ImmutableTile3d.Builder()
                .from(getRoot())
                .content(
                    getRoot()
                        .getContent()
                        .map(
                            content ->
                                new ImmutableWithUri.Builder().uri(implicitContentUri).build()))
                .implicitTiling(
                    getRoot()
                        .getImplicitTiling()
                        .map(
                            implicitTiling ->
                                new ImmutableImplicitTiling.Builder()
                                    .from(implicitTiling)
                                    .subtrees(
                                        new ImmutableWithUri.Builder()
                                            .uri(implicitSubtreeUri)
                                            .build())
                                    .build()))
                .build())
        .build();
  }

  default Tileset3d withUris(String uriPrefix) {
    return new ImmutableTileset3d.Builder().from(this).root(getRoot().withUris(uriPrefix)).build();
  }

  String SCHEMA_REF = "#/components/schemas/Tileset3dTiles";

  @SuppressWarnings("UnstableApiUsage")
  Funnel<Tileset3d> FUNNEL =
      (from, into) -> {
        AssetMetadata.FUNNEL.funnel(from.getAsset(), into);
        from.getGeometricError().ifPresent(into::putFloat);
        Tile3d.FUNNEL.funnel(from.getRoot(), into);
      };

  AssetMetadata getAsset();

  Optional<Float> getGeometricError();

  Tile3d getRoot();

  Optional<Map<String, String>> getSchema();

  Optional<String> getSchemaUri();

  List<String> getExtensionsUsed();

  List<String> getExtensionsRequired();
}
