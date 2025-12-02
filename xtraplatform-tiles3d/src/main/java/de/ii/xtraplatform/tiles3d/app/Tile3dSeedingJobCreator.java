/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.base.domain.LogContext.MARKER;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.jobs.domain.Job;
import de.ii.xtraplatform.jobs.domain.JobProcessor;
import de.ii.xtraplatform.jobs.domain.JobResult;
import de.ii.xtraplatform.jobs.domain.JobSet;
import de.ii.xtraplatform.tiles.domain.SeedingOptions.JobSize;
import de.ii.xtraplatform.tiles.domain.TileMatrixPartitions;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJob;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJobSet;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.AmountFormats;

@Singleton
@AutoBind
public class Tile3dSeedingJobCreator implements JobProcessor<Boolean, Tile3dSeedingJobSet> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dSeedingJobCreator.class);

  private final int concurrency;
  private final EntityRegistry entityRegistry;

  @Inject
  Tile3dSeedingJobCreator(AppContext appContext, EntityRegistry entityRegistry) {
    this.concurrency = appContext.getConfiguration().getBackgroundTasks().getMaxThreads();
    this.entityRegistry = entityRegistry;
  }

  @Override
  public String getJobType() {
    return Tile3dSeedingJobSet.TYPE_SETUP;
  }

  @Override
  public int getPriority() {
    // should be higher than for the processors of the created jobs (VectorSeedingJobProcessor)
    return 1001;
  }

  @Override
  public int getConcurrency(JobSet jobSet) {
    return concurrency;
  }

  @Override
  public JobResult process(Job job, JobSet jobSet, Consumer<Job> pushJob) {
    Tile3dSeedingJobSet seedingJobSet = getSetDetails(jobSet);
    boolean isCleanup = getDetails(job);

    Optional<Tile3dProvider> optionalTileProvider =
        getTileProvider(seedingJobSet.getTileProvider());
    if (optionalTileProvider.isPresent()) {
      Tile3dProvider tileProvider = optionalTileProvider.get();

      if (!tileProvider.seeding().isSupported()) {
        LOGGER.error("Tile provider does not support seeding: {}", tileProvider.getId());
        return JobResult.error("Tile provider does not support seeding"); // early return
      }

      try {

        if (isCleanup) {
          tileProvider.seeding().get().cleanupSeeding(seedingJobSet);

          long duration = Instant.now().getEpochSecond() - jobSet.getStartedAt().get();
          List<String> errors = jobSet.getErrors().get();

          if (!errors.isEmpty() && (LOGGER.isWarnEnabled() || LOGGER.isWarnEnabled(MARKER.JOBS))) {
            LOGGER.warn(
                MARKER.JOBS,
                "{} had {} errors{}",
                jobSet.getLabel(),
                errors.size(),
                jobSet.getDescription().orElse(""));

            if (LOGGER.isDebugEnabled() || LOGGER.isDebugEnabled(MARKER.JOBS)) {
              for (String error : errors) {
                LOGGER.debug(
                    MARKER.JOBS,
                    "{} error: {}{}",
                    jobSet.getLabel(),
                    error,
                    jobSet.getDescription().orElse(""));
              }
            }
          }

          if (LOGGER.isInfoEnabled() || LOGGER.isInfoEnabled(MARKER.JOBS)) {
            LOGGER.info(
                MARKER.JOBS,
                "{} finished in {}{}",
                jobSet.getLabel(),
                pretty(duration),
                jobSet.getDescription().orElse(""));
          }

          return JobResult.success(); // early return
        }

        if (LOGGER.isInfoEnabled() || LOGGER.isInfoEnabled(MARKER.JOBS)) {
          LOGGER.info(
              MARKER.JOBS,
              "{} scheduled (Tilesets: {})",
              jobSet.getLabel(),
              seedingJobSet.getTileSets().keySet());
        }

        Map<String, Map<String, Set<TileMatrixSetLimits>>> coverage =
            tileProvider.seeding().get().getCoverage(seedingJobSet.getTileSetParameters());

        int jobSize = 4;
        switch (Objects.requireNonNullElse(
            tileProvider.seeding().get().getOptions().getJobSize(), JobSize.M)) {
          case S:
            jobSize = 1;
            break;
          case M:
            jobSize = 4;
            break;
          case L:
            jobSize = 16;
            break;
          case XL:
            jobSize = 64;
            break;
        }
        TileMatrixPartitions tileMatrixPartitions = new TileMatrixPartitions(jobSize);

        tileProvider.seeding().get().setupSeeding(seedingJobSet);

        for (String tileSet : seedingJobSet.getTileSets().keySet()) {
          Map<String, Set<TileMatrixSetLimits>> tileMatrixSets =
              coverage.containsKey(tileSet) ? coverage.get(tileSet) : Map.of();

          tileMatrixSets.forEach(
              (tileMatrixSet, limits) -> {
                Set<TileSubMatrix> subMatrices = new LinkedHashSet<>();

                limits.forEach(
                    (limit) -> {
                      subMatrices.addAll(tileMatrixPartitions.getSubMatrices(limit));
                    });

                for (TileSubMatrix subMatrix : subMatrices) {
                  Job job2 =
                      Tile3dSeedingJob.of(
                          jobSet.getPriority(),
                          tileProvider.getId(),
                          tileSet,
                          tileMatrixSet,
                          seedingJobSet.isReseed(),
                          Set.of(subMatrix),
                          Optional.of(seedingJobSet.getTileSetParameters().get(tileSet)),
                          jobSet.getId());

                  pushJob.accept(job2);
                  jobSet.init(job2.getTotal().get());
                  seedingJobSet.init(
                      tileSet, tileMatrixSet, subMatrix.getLevel(), job2.getTotal().get());
                }
              });
        }

        if (jobSet.isDone()) {
          jobSet.getCleanup().ifPresent(pushJob);
          return JobResult.success(); // early return
        }

        if (LOGGER.isDebugEnabled() || LOGGER.isDebugEnabled(MARKER.JOBS)) {
          String processors = getConcurrency(jobSet) + " local";
          LOGGER.debug(
              MARKER.JOBS,
              "{}: processing {} tiles with {} processors",
              jobSet.getLabel(),
              jobSet.getTotal().get(),
              processors);
        }
      } catch (IOException e) {
        return JobResult.error(e.getMessage());
      }
    }

    return JobResult.success();
  }

  @Override
  public Class<Boolean> getDetailsType() {
    return Boolean.class;
  }

  @Override
  public Class<Tile3dSeedingJobSet> getSetDetailsType() {
    return Tile3dSeedingJobSet.class;
  }

  private Optional<Tile3dProvider> getTileProvider(String id) {
    return entityRegistry.getEntity(Tile3dProvider.class, id);
  }

  private static String pretty(long seconds) {
    Duration d = Duration.ofSeconds(seconds);
    return AmountFormats.wordBased(d, Locale.ENGLISH);
  }
}
