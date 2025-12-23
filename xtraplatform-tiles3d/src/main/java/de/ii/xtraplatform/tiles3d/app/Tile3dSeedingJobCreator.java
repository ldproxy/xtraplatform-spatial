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
import de.ii.xtraplatform.jobs.domain.JobQueueMin;
import de.ii.xtraplatform.jobs.domain.JobResult;
import de.ii.xtraplatform.jobs.domain.JobSet;
import de.ii.xtraplatform.tiles.domain.TileMatrixPartitions;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJob;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJobSet;
import de.ii.xtraplatform.tiles3d.domain.TileTree;
import de.ii.xtraplatform.tiles3d.domain.Tileset3dFeatures;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
    this.concurrency = appContext.getConfiguration().getJobConcurrency();
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
  public JobResult process(Job job, JobSet jobSet, JobQueueMin jobQueue) {
    Tile3dSeedingJobSet seedingJobSet = getSetDetails(jobSet, jobQueue);
    boolean isCleanup = getDetails(job, jobQueue);

    Optional<Tile3dProviderFeatures> optionalTileProvider =
        getTileProvider(seedingJobSet.getTileProvider());
    if (optionalTileProvider.isPresent()) {
      Tile3dProviderFeatures tileProvider = optionalTileProvider.get();

      if (!tileProvider.seeding().isSupported()) {
        LOGGER.error("Tile provider does not support seeding: {}", tileProvider.getId());
        return JobResult.error("Tile provider does not support seeding"); // early return
      }

      try {

        if (isCleanup) {
          cleanup(jobSet, tileProvider, seedingJobSet);

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
        final int[] numSubtrees = {0};
        Set<String> tilesets = new HashSet<>();
        Set<Integer> levels = new HashSet<>();

        tileProvider.seeding().get().setupSeeding(seedingJobSet);

        for (String tileSet : seedingJobSet.getTileSets().keySet()) {
          Tileset3dFeatures cfg = tileProvider.getData().getTilesets().get(tileSet);
          Map<String, Set<TileMatrixSetLimits>> tileMatrixSets =
              coverage.containsKey(tileSet) ? coverage.get(tileSet) : Map.of();

          tileMatrixSets.forEach(
              (tileMatrixSet, limits) -> {
                TileTree tileTree = null;

                for (TileMatrixSetLimits limit : limits) {
                  TileTree next = TileTree.from(TileSubMatrix.of(limit), cfg.getSubtreeLevels());
                  tileTree = tileTree == null ? next : tileTree.merge(next);
                }

                if (Objects.nonNull(tileTree)) {
                  int jobSize = getJobSize(tileTree);
                  TileMatrixPartitions tileMatrixPartitions = new TileMatrixPartitions(jobSize);

                  Job rootJob =
                      tileTree.accept(
                          (tt, followUps) -> {
                            Job subtreeJob =
                                Tile3dSeedingJob.subtree(
                                    jobSet.getPriority(),
                                    tileProvider.getId(),
                                    tileSet,
                                    tileMatrixSet,
                                    seedingJobSet.isReseed(),
                                    Set.of(tt.toSubMatrix()),
                                    Optional.of(seedingJobSet.getTileSetParameters().get(tileSet)),
                                    jobSet.getId());

                            int total = subtreeJob.getTotal().get();
                            numSubtrees[0] += total;
                            subtreeJob = subtreeJob.with(followUps);

                            jobQueue.initJobSet(
                                jobSet,
                                total,
                                Map.<String, Object>of(
                                    "tileSet",
                                    tileSet,
                                    "tileMatrixSet",
                                    tileMatrixSet,
                                    "level",
                                    tt.getLevel(),
                                    "count",
                                    total,
                                    "isFirstTileset",
                                    tilesets.add(tileSet),
                                    "isFirstLevel",
                                    levels.add(tt.getLevel())));

                            for (TileSubMatrix content : tt.getContent()) {
                              for (TileSubMatrix partition :
                                  tileMatrixPartitions.getSubMatrices(content.toLimits())) {
                                Job contentJob =
                                    Tile3dSeedingJob.content(
                                        jobSet.getPriority(),
                                        tileProvider.getId(),
                                        tileSet,
                                        tileMatrixSet,
                                        seedingJobSet.isReseed(),
                                        Set.of(partition),
                                        Optional.of(
                                            seedingJobSet.getTileSetParameters().get(tileSet)),
                                        jobSet.getId());

                                subtreeJob = subtreeJob.with(contentJob);

                                jobQueue.initJobSet(
                                    jobSet,
                                    contentJob.getTotal().get(),
                                    Map.<String, Object>of(
                                        "tileSet",
                                        tileSet,
                                        "tileMatrixSet",
                                        tileMatrixSet,
                                        "level",
                                        content.getLevel(),
                                        "count",
                                        contentJob.getTotal().get(),
                                        "isFirstLevel",
                                        levels.add(content.getLevel())));
                              }
                            }

                            return subtreeJob;
                          });

                  jobQueue.push(rootJob);
                }
              });
        }

        if (jobSet.isDone()) {
          jobSet.getCleanup().ifPresent(jobQueue::push);
          return JobResult.success(); // early return
        }

        if (LOGGER.isDebugEnabled() || LOGGER.isDebugEnabled(MARKER.JOBS)) {
          String processors = getConcurrency(jobSet) + " local";
          LOGGER.debug(
              MARKER.JOBS,
              "{}: processing {} subtrees and {} tiles with {} processors",
              jobSet.getLabel(),
              numSubtrees[0],
              Math.max(jobSet.getTotal().get() - numSubtrees[0], 0),
              processors);
        }
      } catch (IOException e) {
        return JobResult.error(e.getMessage());
      }
    }

    return JobResult.success();
  }

  private static int getJobSize(TileTree tileTree) {
    long numberOfTiles = tileTree.getNumberOfTiles();
    if (numberOfTiles <= 32) {
      return 1;
    } else if (numberOfTiles <= 128) {
      return 2;
    } else if (numberOfTiles <= 512) {
      return 4;
    } else if (numberOfTiles <= 1024) {
      return 8;
    } else if (numberOfTiles <= 4096) {
      return 16;
    } else if (numberOfTiles <= 16384) {
      return 64;
    }
    return 256;
  }

  private void cleanup(
      JobSet jobSet, Tile3dProvider tileProvider, Tile3dSeedingJobSet seedingJobSet)
      throws IOException {
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
  }

  @Override
  public Class<Boolean> getDetailsType() {
    return Boolean.class;
  }

  @Override
  public Class<Tile3dSeedingJobSet> getSetDetailsType() {
    return Tile3dSeedingJobSet.class;
  }

  @Override
  public Map<String, Class<?>> getJobTypes() {
    return Map.of(
        Tile3dSeedingJobSet.TYPE,
        Tile3dSeedingJobSet.class,
        Tile3dSeedingJobSet.TYPE_SETUP,
        Boolean.class);
  }

  private Optional<Tile3dProviderFeatures> getTileProvider(String id) {
    return entityRegistry.getEntity(Tile3dProviderFeatures.class, id);
  }

  private static String pretty(long seconds) {
    Duration d = Duration.ofSeconds(seconds);
    return AmountFormats.wordBased(d, Locale.ENGLISH);
  }
}
