/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain.spec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.Funnel;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTileset3d.Builder.class)
@JsonInclude(Include.NON_EMPTY)
public interface Tileset3d {

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

  Map<String, Object> getProperties();

  Map<String, Object> getSchema();

  Optional<String> getSchemaUri();

  Map<String, Object> getStatistics();

  List<Map<String, Object>> getGroups();

  Map<String, Object> getMetadata();

  Optional<Float> getGeometricError();

  Tile3d getRoot();

  List<String> getExtensionsUsed();

  List<String> getExtensionsRequired();

  Map<String, Object> getExtensions();

  Map<String, Object> getExtras();

  default void validate(Path source) {
    if (Objects.isNull(getAsset())
        || (!Objects.equals(getAsset().getVersion(), "1.0")
            && !Objects.equals(getAsset().getVersion(), "1.1"))) {
      throw new IllegalStateException("Invalid version found in 3D Tiles tileset file: " + source);
    }

    if (Objects.isNull(getRoot()) || Objects.isNull(getRoot().getBoundingVolume())) {
      throw new IllegalStateException(
          "Invalid root tile found in 3D Tiles tileset file: " + source);
    }
  }
}
