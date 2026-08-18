/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import de.ii.xtralink.jobs.JobConfiguration;
import de.ii.xtraplatform.jobs.domain.JobProgress;
import de.ii.xtraplatform.tiles.domain.ImmutableTileSeedingJob.Builder;
import de.ii.xtraplatform.xtralink.domain.JobContext.JobContextEntity;
import de.ii.xtraplatform.xtralink.domain.JobInputs;
import de.ii.xtraplatform.xtralink.domain.Jobs;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTileSeedingJob.Builder.class)
public interface TileSeedingJob extends JobInputs {

  String TYPE = "tile-seeding";
  String TYPE_SETUP = type("setup");
  String TYPE_CLEANUP = type("cleanup");
  String LABEL = "Tile cache seeding";

  static String type(String... parts) {
    return String.join(":", TYPE, String.join(":", parts));
  }

  static JobConfiguration of(
      String tileProvider,
      Map<String, TileGenerationParameters> tileSets,
      boolean reseed,
      int priority) {
    ImmutableTileSeedingJob tileSeedingJob =
        new Builder()
            .tileProvider(tileProvider)
            .tileSets(TilesetDetailsXL.of(tileSets))
            .isReseed(reseed)
            .build();
    return Jobs.create(
        TYPE,
        priority,
        LABEL,
        String.format(" (Tilesets: %s)", tileSets.keySet()),
        tileSeedingJob,
        new JobContextEntity(tileProvider),
        tileSeedingJob.getTileSets().entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> e.getValue().getProgress2())));
  }

  String getTileProvider();

  Map<String, TilesetDetailsXL> getTileSets();

  @JsonIgnore
  @Value.Lazy
  default Map<String, TileGenerationParameters> getTileSetParameters() {
    return getTileSets().entrySet().stream()
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> e.getValue().getParameters()));
  }

  boolean isReseed();

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableTilesetDetailsXL.Builder.class)
  interface TilesetDetailsXL {

    static Map<String, TilesetDetailsXL> of(Map<String, TileGenerationParameters> tilesets) {
      return tilesets.entrySet().stream()
          .collect(
              ImmutableMap.toImmutableMap(
                  Map.Entry::getKey, e -> TilesetDetailsXL.of(e.getValue())));
    }

    static TilesetDetailsXL of(TileGenerationParameters parameters) {
      return new ImmutableTilesetDetailsXL.Builder()
          .parameters(parameters)
          .progress2(new ImmutableTilesetProgressXL2.Builder().build())
          .build();
    }

    TileGenerationParameters getParameters();

    @Nullable
    @JsonIgnore
    TilesetProgressXL2 getProgress2();
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableTilesetProgressXL2.Builder.class)
  interface TilesetProgressXL2 extends JobProgress {
    Map<String, List<Integer>> getLevels();
  }
}
