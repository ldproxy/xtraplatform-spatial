/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import de.ii.xtralink.jobs.JobConfiguration;
import de.ii.xtraplatform.xtralink.domain.JobContext.JobContextEntity;
import de.ii.xtraplatform.xtralink.domain.JobInputs;
import de.ii.xtraplatform.xtralink.domain.Jobs;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTile3dSeedingJob.Builder.class)
public interface Tile3dSeedingJob extends JobInputs {

  String TYPE = "tile3d-seeding";
  String TYPE_SETUP = type("setup");
  String TYPE_CLEANUP = type("cleanup");
  String LABEL = "3D Tiles cache seeding";

  static String type(String... parts) {
    return String.join(":", TYPE, String.join(":", parts));
  }

  static JobConfiguration of(
      String tileProvider,
      Map<String, Tile3dGenerationParameters> tileSets,
      boolean reseed,
      int priority) {
    return Jobs.create(
        TYPE,
        priority,
        LABEL,
        String.format(" (Tilesets: %s)", tileSets.keySet()),
        new ImmutableTile3dSeedingJob.Builder()
            .tileProvider(tileProvider)
            .tileSets(Tileset3dDetails.of(tileSets))
            .isReseed(reseed)
            .build(),
        new JobContextEntity(tileProvider),
        Map.of(),
        // setup queues the seeding partials itself, they depend on the tiles to seed
        List.of(),
        Optional.of(3600));
  }

  String getTileProvider();

  Map<String, Tileset3dDetails> getTileSets();

  @JsonIgnore
  @Value.Lazy
  default Map<String, Tile3dGenerationParameters> getTileSetParameters() {
    return getTileSets().entrySet().stream()
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> e.getValue().getParameters()));
  }

  boolean isReseed();

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableTileset3dDetails.Builder.class)
  @SuppressWarnings("PMD.ImplicitFunctionalInterface")
  interface Tileset3dDetails {

    static Map<String, Tileset3dDetails> of(Map<String, Tile3dGenerationParameters> tilesets) {
      return tilesets.entrySet().stream()
          .collect(
              ImmutableMap.toImmutableMap(
                  Map.Entry::getKey, e -> Tileset3dDetails.of(e.getValue())));
    }

    static Tileset3dDetails of(Tile3dGenerationParameters parameters) {
      return new ImmutableTileset3dDetails.Builder().parameters(parameters).build();
    }

    Tile3dGenerationParameters getParameters();
  }
}
