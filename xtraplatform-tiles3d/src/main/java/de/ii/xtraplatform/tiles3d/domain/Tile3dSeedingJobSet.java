/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTile3dSeedingJobSet.Builder.class)
public interface Tile3dSeedingJobSet extends JobSetDetails {

  String TYPE = "tile3d-seeding";
  String TYPE_SETUP = type("setup");
  String LABEL = "3D Tiles cache seeding";
  List<Integer> INITIAL_LEVELS = IntStream.range(0, 24).map(i -> -1).boxed().toList();

  static String type(String... parts) {
    return String.join(":", TYPE, String.join(":", parts));
  }

  static JobSet of(
      String tileProvider,
      Map<String, Tile3dGenerationParameters> tileSets,
      boolean reseed,
      int priority) {
    return JobSet.of(
            TYPE,
            priority,
            tileProvider,
            LABEL,
            String.format(" (Tilesets: %s)", tileSets.keySet()),
            new ImmutableTile3dSeedingJobSet.Builder()
                .tileProvider(tileProvider)
                .tileSets(Tileset3dDetails.of(tileSets))
                .isReseed(reseed)
                .build())
        .with(Job.of(TYPE_SETUP, priority, false), Job.of(TYPE_SETUP, priority, true));
  }

  String getTileProvider();

  Map<String, Tileset3dDetails> getTileSets();

  @JsonIgnore
  @Value.Lazy
  @Override
  default String getLabel() {
    return "Tilesets: " + getTileSets().keySet();
  }

  @JsonIgnore
  @Value.Lazy
  default Map<String, Tile3dGenerationParameters> getTileSetParameters() {
    return getTileSets().entrySet().stream()
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> e.getValue().getParameters()));
  }

  boolean isReseed();

  default void init(String tileSet, String tileMatrixSet, int level, int count) {
    Tileset3dProgress progress = getTileSets().get(tileSet).getProgress();

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

  @Override
  default void init(Map<String, Object> parameters) {
    if (parameters.containsKey("tileSet")
        && parameters.containsKey("tileMatrixSet")
        && parameters.containsKey("level")
        && parameters.containsKey("count")) {
      init(
          (String) parameters.get("tileSet"),
          (String) parameters.get("tileMatrixSet"),
          parameters.get("level") instanceof Integer
              ? (Integer) parameters.get("level")
              : Integer.parseInt((String) parameters.get("level")),
          parameters.get("count") instanceof Integer
              ? (Integer) parameters.get("count")
              : Integer.parseInt((String) parameters.get("count")));
    }
  }

  @Override
  default Map<String, Object> initJson(Map<String, Object> params) {
    Map<String, Object> jsonPathUpdates = new LinkedHashMap<>();

    if (params.containsKey("tileSet") && params.get("count") instanceof Integer) {
      int delta = (Integer) params.get("count");
      boolean isFirstTileset = Objects.equals(params.get("isFirstTileset"), true);
      int tilesetDelta = isFirstTileset ? 1 : 0;

      jsonPathUpdates.put(
          "$.details.tileSets.%s.progress.total".formatted(params.get("tileSet")),
          delta + tilesetDelta);

      if (params.containsKey("tileMatrixSet")) {
        if (isFirstTileset) {
          jsonPathUpdates.put(
              "$.details.tileSets.%s.progress.levels.%s"
                  .formatted(params.get("tileSet"), params.get("tileMatrixSet")),
              INITIAL_LEVELS);
        }

        if (params.containsKey("level")) {
          int levelDelta = Objects.equals(params.get("isFirstLevel"), true) ? 1 : 0;

          jsonPathUpdates.put(
              "$.details.tileSets.%s.progress.levels.%s[%s]"
                  .formatted(
                      params.get("tileSet"), params.get("tileMatrixSet"), params.get("level")),
              delta + levelDelta);
        }
      }
    }

    return jsonPathUpdates;
  }

  default void update(String tileSet, String tileMatrixSet, int level, int delta) {
    Tileset3dProgress progress = getTileSets().get(tileSet).getProgress();

    progress.getCurrent().addAndGet(delta);

    if (progress.getLevels().containsKey(tileMatrixSet)) {
      progress.getLevels().get(tileMatrixSet).addAndGet(level, -1 * delta);
    }
  }

  @Override
  default void update(Map<String, Object> parameters) {
    if (parameters.containsKey("tileSet")
        && parameters.containsKey("tileMatrixSet")
        && parameters.containsKey("level")
        && parameters.containsKey("delta")) {
      update(
          (String) parameters.get("tileSet"),
          (String) parameters.get("tileMatrixSet"),
          parameters.get("level") instanceof Integer
              ? (Integer) parameters.get("level")
              : Integer.parseInt((String) parameters.get("level")),
          parameters.get("delta") instanceof Integer
              ? (Integer) parameters.get("delta")
              : Integer.parseInt((String) parameters.get("delta")));
    }
  }

  @Override
  default Map<String, Object> updateJson(Map<String, Object> detailParameters) {
    Map<String, Object> jsonPathUpdates = new LinkedHashMap<>();

    if (detailParameters.containsKey("tileSet")
        && detailParameters.get("delta") instanceof Integer) {
      int delta = (Integer) detailParameters.get("delta");

      jsonPathUpdates.put(
          "$.details.tileSets.%s.progress.current".formatted(detailParameters.get("tileSet")),
          delta);

      if (detailParameters.containsKey("tileMatrixSet") && detailParameters.containsKey("level")) {
        jsonPathUpdates.put(
            "$.details.tileSets.%s.progress.levels.%s[%s]"
                .formatted(
                    detailParameters.get("tileSet"),
                    detailParameters.get("tileMatrixSet"),
                    detailParameters.get("level")),
            -1 * delta);
      }
    }

    return jsonPathUpdates;
  }

  @Override
  default void reset(Job job) {
    if (job.getDetails() instanceof Tile3dSeedingJob) {
      Tile3dSeedingJob details = (Tile3dSeedingJob) job.getDetails();

      Tileset3dProgress progress = getTileSets().get(details.getTileSet()).getProgress();

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
  @JsonDeserialize(builder = ImmutableTileset3dDetails.Builder.class)
  interface Tileset3dDetails {

    static Map<String, Tileset3dDetails> of(Map<String, Tile3dGenerationParameters> tilesets) {
      return tilesets.entrySet().stream()
          .collect(
              ImmutableMap.toImmutableMap(
                  Map.Entry::getKey, e -> Tileset3dDetails.of(e.getValue())));
    }

    static Tileset3dDetails of(Tile3dGenerationParameters parameters) {
      return new ImmutableTileset3dDetails.Builder()
          .parameters(parameters)
          .progress(new ImmutableTileset3dProgress.Builder().levels(new LinkedHashMap<>()).build())
          .build();
    }

    Tile3dGenerationParameters getParameters();

    Tileset3dProgress getProgress();
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableTileset3dProgress.Builder.class)
  interface Tileset3dProgress extends JobProgress {
    @JsonIgnore
    @Nullable
    LinkedHashMap<String, AtomicIntegerArray> getLevels();

    @JsonProperty(value = "levels", access = Access.WRITE_ONLY)
    Map<String, List<Integer>> getLevelsInput();

    @JsonProperty(value = "levels", access = Access.READ_ONLY)
    default Map<String, List<Integer>> getLevelsOutput() {
      if (getLevels() == null) {
        return Map.of();
      }

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

    @Value.Check
    default Tileset3dProgress deser() {
      // Ensure that levels is initialized
      if (getLevels() == null) {
        LinkedHashMap<String, AtomicIntegerArray> levelsMap = new LinkedHashMap<>();
        for (Map.Entry<String, List<Integer>> entry : getLevelsInput().entrySet()) {
          List<Integer> levelList = entry.getValue();
          AtomicIntegerArray atomicArray = new AtomicIntegerArray(levelList.size());
          for (int i = 0; i < levelList.size(); i++) {
            atomicArray.set(i, levelList.get(i));
          }
          levelsMap.put(entry.getKey(), atomicArray);
        }

        return new ImmutableTileset3dProgress.Builder().from(this).levels(levelsMap).build();
      }
      return this;
    }
  }
}
