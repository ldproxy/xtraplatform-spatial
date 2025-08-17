/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.jobs.domain.Job;
import de.ii.xtraplatform.jobs.domain.JobProgress;
import de.ii.xtraplatform.jobs.domain.JobSet;
import de.ii.xtraplatform.jobs.domain.JobSet.JobSetDetails;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.immutables.value.Value;

@Value.Immutable
public interface TileSeedingJobSet extends JobSetDetails {

  String TYPE = "tile-seeding";
  String TYPE_SETUP = type("setup");
  String LABEL = "Tile cache seeding";

  static String type(String... parts) {
    return String.join(":", TYPE, String.join(":", parts));
  }

  static JobSet of(
      String tileProvider,
      Map<String, TileGenerationParameters> tileSets,
      boolean reseed,
      int priority) {
    return JobSet.of(
            TYPE,
            priority,
            tileProvider,
            LABEL,
            String.format(" (Tilesets: %s)", tileSets.keySet()),
            new ImmutableTileSeedingJobSet.Builder()
                .tileProvider(tileProvider)
                .tileSets(TilesetDetails.of(tileSets))
                .isReseed(reseed)
                .build())
        .with(Job.of(TYPE_SETUP, priority, false), Job.of(TYPE_SETUP, priority, true));
  }

  static JobSet with(JobSet jobSet, Map<String, TileGenerationParameters> tileSets) {
    TileSeedingJobSet details =
        new ImmutableTileSeedingJobSet.Builder()
            .from((TileSeedingJobSet) jobSet.getDetails())
            .putAllTileSets(TilesetDetails.of(tileSets))
            .build();
    return jobSet.with(String.format(" (Tilesets: %s)", details.getTileSets().keySet()), details);
  }

  String getTileProvider();

  Map<String, TilesetDetails> getTileSets();

  @JsonIgnore
  @Value.Lazy
  @Override
  default String getLabel() {
    return "Tilesets: " + getTileSets().keySet();
  }

  @JsonIgnore
  @Value.Lazy
  default Map<String, TileGenerationParameters> getTileSetParameters() {
    return getTileSets().entrySet().stream()
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> e.getValue().getParameters()));
  }

  boolean isReseed();

  default void init(String tileSet, String tileMatrixSet, int level, int count) {
    TilesetProgress progress = getTileSets().get(tileSet).getProgress();

    progress.getTotal().updateAndGet(old -> old == -1 ? count : old + count);

    if (!progress.getLevels().containsKey(tileMatrixSet)) {
      int[] levelProgress = new int[24];
      Arrays.fill(levelProgress, -1);
      progress.getLevels().put(tileMatrixSet, new AtomicIntegerArray(levelProgress));
    }

    progress
        .getLevels()
        .get(tileMatrixSet)
        .getAndUpdate(level, old -> old == -1 ? count : old + count);
  }

  default void update(String tileSet, String tileMatrixSet, int level, int delta) {
    TilesetProgress progress = getTileSets().get(tileSet).getProgress();

    progress.getCurrent().addAndGet(delta);

    if (progress.getLevels().containsKey(tileMatrixSet)) {
      progress.getLevels().get(tileMatrixSet).addAndGet(level, -1 * delta);
    }
  }

  @Override
  default void update(Map<String, String> parameters) {
    if (parameters.containsKey("tileSet")
        && parameters.containsKey("tileMatrixSet")
        && parameters.containsKey("level")
        && parameters.containsKey("delta")) {
      update(
          parameters.get("tileSet"),
          parameters.get("tileMatrixSet"),
          Integer.parseInt(parameters.get("level")),
          Integer.parseInt(parameters.get("delta")));
    }
  }

  @Override
  default void reset(Job job) {
    if (job.getDetails() instanceof TileSeedingJob) {
      TileSeedingJob details = (TileSeedingJob) job.getDetails();

      TilesetProgress progress = getTileSets().get(details.getTileSet()).getProgress();

      progress.getCurrent().addAndGet(-(job.getCurrent().get()));

      if (progress.getLevels().containsKey(details.getTileMatrixSet())) {
        int level = details.getSubMatrices().get(0).getLevel();
        progress
            .getLevels()
            .get(details.getTileMatrixSet())
            .addAndGet(level, job.getCurrent().get());
      }
    }
  }

  @Value.Immutable
  interface TilesetDetails {

    static Map<String, TilesetDetails> of(Map<String, TileGenerationParameters> tilesets) {
      return tilesets.entrySet().stream()
          .collect(
              ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> TilesetDetails.of(e.getValue())));
    }

    static TilesetDetails of(TileGenerationParameters parameters) {
      return new ImmutableTilesetDetails.Builder()
          .parameters(parameters)
          .progress(new ImmutableTilesetProgress.Builder().levels(new LinkedHashMap<>()).build())
          .build();
    }

    TileGenerationParameters getParameters();

    TilesetProgress getProgress();
  }

  @Value.Immutable
  interface TilesetProgress extends JobProgress {
    @JsonIgnore
    LinkedHashMap<String, AtomicIntegerArray> getLevels();

    @JsonProperty("levels")
    default Map<String, List<Integer>> getLevelsArray() {

      return getLevels().entrySet().stream()
          .map(
              e -> {
                List<Integer> levels = new ArrayList<>();
                for (int i = 0; i < e.getValue().length(); i++) {
                  levels.add(e.getValue().get(i));
                }
                return Map.entry(e.getKey(), levels);
              })
          .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
  }
}
