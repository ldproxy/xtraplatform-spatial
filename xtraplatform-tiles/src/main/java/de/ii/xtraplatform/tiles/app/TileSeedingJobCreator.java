/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtralink.jobs.Job;
import de.ii.xtralink.jobs.JobResult;
import de.ii.xtralink.jobs.PartialJob;
import de.ii.xtralink.jobs.PartialJobConfiguration;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.base.domain.LogContext.MARKER;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.tiles.domain.TileGenerationParameters;
import de.ii.xtraplatform.tiles.domain.TileMatrixPartitions;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import de.ii.xtraplatform.tiles.domain.TileProvider;
import de.ii.xtraplatform.tiles.domain.TileSeedingJob;
import de.ii.xtraplatform.tiles.domain.TileSeedingPartialJob;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import de.ii.xtraplatform.xtralink.domain.JobContext.JobContextNone;
import de.ii.xtraplatform.xtralink.domain.JobProcessing;
import de.ii.xtraplatform.xtralink.domain.JobProcessor;
import de.ii.xtraplatform.xtralink.domain.JobProcessorBase;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.AmountFormats;

@Singleton
@AutoBind(interfaces = JobProcessorBase.class)
public class TileSeedingJobCreator implements JobProcessor<TileSeedingJob, JobContextNone> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileSeedingJobCreator.class);

  private final int concurrency;
  private final EntityRegistry entityRegistry;

  @Inject
  TileSeedingJobCreator(AppContext appContext, EntityRegistry entityRegistry) {
    this.concurrency = appContext.getConfiguration().getJobConcurrency();
    this.entityRegistry = entityRegistry;
  }

  @Override
  public Set<String> getKinds() {
    return Set.of(TileSeedingJob.TYPE_SETUP, TileSeedingJob.TYPE_CLEANUP);
  }

  @Override
  public int getPriority() {
    // should be higher than for the processors of the created jobs (VectorSeedingJobProcessor)
    return 1001;
  }

  @Override
  public JobResult process(PartialJob partialJob, Job jobSet, JobProcessing jobs) throws Exception {
    boolean isCleanup = TileSeedingJob.TYPE_CLEANUP.equals(partialJob.kind());
    TileSeedingJob seedingJobSet = getInputs(jobSet, jobs);

    final int[] progressTotal = {0};
    Map<String, Map<String, List<Integer>>> progressDetails = new LinkedHashMap<>();
    for (String tileSet : seedingJobSet.getTileSets().keySet()) {
      Map<String, List<Integer>> tileMatrixSetProgress = new LinkedHashMap<>();
      progressDetails.put(tileSet, tileMatrixSetProgress);
    }

    Optional<TileProvider> optionalTileProvider = getTileProvider(seedingJobSet.getTileProvider());
    if (optionalTileProvider.isPresent()) {
      TileProvider tileProvider = optionalTileProvider.get();

      if (!tileProvider.seeding().isSupported()) {
        LOGGER.error("Tile provider does not support seeding: {}", tileProvider.getId());
        return jobs.failure("Tile provider does not support seeding");
      }

      try {

        if (isCleanup) {
          tileProvider.seeding().get().cleanupSeeding(seedingJobSet);

          long duration = Instant.now().toEpochMilli() - jobSet.startedAt();
          List<String> errors = jobSet.errors();

          if (!errors.isEmpty() && (LOGGER.isWarnEnabled() || LOGGER.isWarnEnabled(MARKER.JOBS))) {
            LOGGER.warn(
                MARKER.JOBS,
                "{} had {} errors{}",
                jobSet.label(),
                errors.size(),
                jobSet.description());

            if (LOGGER.isDebugEnabled() || LOGGER.isDebugEnabled(MARKER.JOBS)) {
              for (String error : errors) {
                LOGGER.debug(
                    MARKER.JOBS, "{} error: {}{}", jobSet.label(), error, jobSet.description());
              }
            }
          }

          if (LOGGER.isInfoEnabled() || LOGGER.isInfoEnabled(MARKER.JOBS)) {
            LOGGER.info(
                MARKER.JOBS,
                "{} finished in {}{}",
                jobSet.label(),
                prettyDuration(duration),
                jobSet.description());
          }

          return jobs.success();
        }

        if (LOGGER.isInfoEnabled() || LOGGER.isInfoEnabled(MARKER.JOBS)) {
          LOGGER.info(
              MARKER.JOBS,
              "{} scheduled (Tilesets: {})",
              jobSet.label(),
              seedingJobSet.getTileSets().keySet());
        }

        Map<String, Map<String, Set<TileMatrixSetLimits>>> coverage =
            tileProvider.seeding().get().getCoverage(seedingJobSet.getTileSetParameters());
        Map<String, Map<String, Set<TileMatrixSetLimits>>> rasterCoverage =
            tileProvider.seeding().get().getRasterCoverage(seedingJobSet.getTileSetParameters());
        TileMatrixPartitions tileStorePartitions =
            new TileMatrixPartitions(
                tileProvider.seeding().get().getOptions().getEffectiveJobSize());

        Map<String, List<String>> rasterForVector =
            seedingJobSet.getTileSets().entrySet().stream()
                .map(
                    entry ->
                        Map.entry(
                            entry.getKey(),
                            tileProvider.access().get().getMapStyles(entry.getKey()).stream()
                                .map(
                                    style ->
                                        tileProvider
                                            .access()
                                            .get()
                                            .getMapStyleTileset(entry.getKey(), style))
                                .collect(Collectors.toList())))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

        Map<String, TileGenerationParameters> rasterForVectorTilesets =
            seedingJobSet.getTileSetParameters().entrySet().stream()
                .flatMap(
                    entry ->
                        rasterForVector.get(entry.getKey()).stream()
                            .map(rasterTileset -> Map.entry(rasterTileset, entry.getValue())))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        Map<String, Map<String, Set<TileMatrixSetLimits>>> rasterForVectorCoverage =
            tileProvider.seeding().get().getRasterCoverage(rasterForVectorTilesets);

        tileProvider.seeding().get().setupSeeding(seedingJobSet);

        boolean allRaster = true;
        boolean someRaster = false;

        for (String tileSet : seedingJobSet.getTileSets().keySet()) {
          Map<String, Set<TileMatrixSetLimits>> tileMatrixSets =
              coverage.containsKey(tileSet)
                  ? coverage.get(tileSet)
                  : rasterCoverage.containsKey(tileSet) ? rasterCoverage.get(tileSet) : Map.of();
          boolean isRaster = rasterCoverage.containsKey(tileSet);

          if (isRaster) {
            someRaster = true;
          } else {
            allRaster = false;
          }

          tileMatrixSets.forEach(
              (tileMatrixSet, limits) -> {
                Set<TileSubMatrix> subMatrices = new LinkedHashSet<>();

                limits.forEach(
                    (limit) -> {
                      subMatrices.addAll(tileStorePartitions.getSubMatrices(limit));
                    });

                for (TileSubMatrix subMatrix : subMatrices) {
                  PartialJobConfiguration partial =
                      isRaster
                          ? TileSeedingPartialJob.raster(
                              jobSet.priority(),
                              tileProvider.getId(),
                              tileSet,
                              tileMatrixSet,
                              seedingJobSet.isReseed(),
                              Set.of(subMatrix),
                              jobSet.id(),
                              tileProvider
                                  .seeding()
                                  .get()
                                  .getRasterStorageInfo(tileSet, tileMatrixSet, subMatrix))
                          : TileSeedingPartialJob.of(
                              jobSet.priority(),
                              tileProvider.getId(),
                              tileSet,
                              tileMatrixSet,
                              seedingJobSet.isReseed(),
                              Set.of(subMatrix),
                              Optional.of(seedingJobSet.getTileSetParameters().get(tileSet)),
                              jobSet.id());

                  progressTotal[0] += partial.progress().total();
                  List<Integer> progressLevels =
                      progressDetails
                          .get(tileSet)
                          .computeIfAbsent(
                              tileMatrixSet,
                              k ->
                                  IntStream.range(0, 24)
                                      .mapToObj(i -> -1)
                                      .collect(Collectors.toCollection(ArrayList::new)));
                  int old = progressLevels.get(subMatrix.getLevel());
                  int count = partial.progress().total();
                  progressLevels.set(subMatrix.getLevel(), old == -1 ? count : old + count);

                  jobs.push(partial);
                }
              });
        }

        jobs.init(jobSet.id(), progressTotal[0], progressDetails);

        if (LOGGER.isDebugEnabled() || LOGGER.isDebugEnabled(MARKER.JOBS)) {
          String processors =
              allRaster
                  ? "remote"
                  : someRaster ? "remote and " + concurrency + " local" : concurrency + " local";
          LOGGER.debug(
              MARKER.JOBS,
              "{}: processing {} tiles with {} processors",
              jobSet.label(),
              progressTotal[0],
              processors);
        }
      } catch (IOException e) {
        return jobs.failure(e.getMessage());
      }
    }

    return jobs.success();
  }

  @Override
  public Class<TileSeedingJob> getInputsClass() {
    return TileSeedingJob.class;
  }

  @Override
  public Class<JobContextNone> getPartialContextClass() {
    return JobContextNone.class;
  }

  private Optional<TileProvider> getTileProvider(String id) {
    return entityRegistry.getEntity(TileProvider.class, id);
  }

  private static String prettyDuration(long millis) {
    Duration d = millis > 1000 ? Duration.ofSeconds(millis / 1000) : Duration.ofMillis(millis);
    return AmountFormats.wordBased(d, Locale.ENGLISH);
  }
}
