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
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTileset3d.Builder.class)
public interface Tileset3d {

  default Tileset3d withUris(String contentUri, String subtreeUri) {
    return new ImmutableTileset3d.Builder()
        .from(this)
        .root(
            new ImmutableTile3d.Builder()
                .from(getRoot())
                .content(new ImmutableWithUri.Builder().uri(contentUri).build())
                .implicitTiling(
                    new ImmutableImplicitTiling.Builder()
                        .from(getRoot().getImplicitTiling())
                        .subtrees(new ImmutableWithUri.Builder().uri(subtreeUri).build())
                        .build())
                .build())
        .build();
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

  Optional<String> getSchemaUri();

  List<String> getExtensionsUsed();

  List<String> getExtensionsRequired();
}
