/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtralink.jobs.Job;
import de.ii.xtralink.jobs.JobResult;
import de.ii.xtralink.jobs.PartialJob;
import de.ii.xtralink.jobs.PartialJobConfiguration;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.base.domain.LogContext.MARKER;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.tiles.domain.TileMatrixPartitions;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJob;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingPartialJob;
import de.ii.xtraplatform.tiles3d.domain.TileTree;
import de.ii.xtraplatform.tiles3d.domain.Tileset3dFeatures;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.AmountFormats;

@Singleton
@AutoBind(interfaces = JobProcessorBase.class)
@SuppressWarnings("PMD.CouplingBetweenObjects")
public class Tile3dSeedingJobCreator implements JobProcessor<Tile3dSeedingJob, JobContextNone> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dSeedingJobCreator.class);

  private final int concurrency;
  private final EntityRegistry entityRegistry;

  @Inject
  Tile3dSeedingJobCreator(AppContext appContext, EntityRegistry entityRegistry) {
    this.concurrency = appContext.getConfiguration().getJobConcurrency();
    this.entityRegistry = entityRegistry;
  }

  @Override
  public Set<String> getKinds() {
    return Set.of(Tile3dSeedingJob.TYPE_SETUP, Tile3dSeedingJob.TYPE_CLEANUP);
  }

  @Override
  public int getPriority() {
    // should be higher than for the processors of the created jobs (Tile3dJobProcessor)
    return 1001;
  }

  @Override
  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  public JobResult process(PartialJob partialJob, Job job, JobProcessing jobs) throws Exception {
    boolean isCleanup = Tile3dSeedingJob.TYPE_CLEANUP.equals(partialJob.kind());
    Tile3dSeedingJob seedingJob = getInputs(job, jobs);

    Optional<Tile3dProviderFeatures> optionalTileProvider =
        getTileProvider(seedingJob.getTileProvider());
    if (optionalTileProvider.isPresent()) {
      Tile3dProviderFeatures tileProvider = optionalTileProvider.get();

      if (!tileProvider.seeding().isSupported()) {
        LOGGER.error("Tile provider does not support seeding: {}", tileProvider.getId());
        return jobs.failure("Tile provider does not support seeding"); // early return
      }

      try {

        if (isCleanup) {
          cleanup(job, tileProvider, seedingJob);

          return jobs.success(); // early return
        }

        Map<String, Map<String, Set<TileMatrixSetLimits>>> coverage =
            tileProvider.seeding().get().getCoverage(seedingJob.getTileSetParameters());
        final int[] numSubtrees = {0};
        Map<String, Integer> tileSetTotals = new LinkedHashMap<>();
        Map<String, Map<String, List<Integer>>> tileSetLevels = new LinkedHashMap<>();
        Map<Integer, TileMatrixPartitions> tileMatrixPartitionsByJobSize = new HashMap<>();
        List<PartialJobConfiguration> rootJobs = new ArrayList<>();

        tileProvider.seeding().get().setupSeeding(seedingJob);

        for (String tileSet : seedingJob.getTileSets().keySet()) {
          Tileset3dFeatures cfg = tileProvider.getData().getTilesets().get(tileSet);
          Map<String, Set<TileMatrixSetLimits>> tileMatrixSets =
              coverage.containsKey(tileSet) ? coverage.get(tileSet) : Map.of();
          tileSetTotals.put(tileSet, 0);
          tileSetLevels.put(tileSet, new LinkedHashMap<>());

          tileMatrixSets.forEach(
              (tileMatrixSet, limits) -> {
                Tile3dSeedingPartialJob.Context jobContext =
                    createJobContext(tileProvider, seedingJob, job, tileSet, tileMatrixSet);
                TileTree tileTree = null;

                for (TileMatrixSetLimits limit : limits) {
                  tileTree = mergeTileTree(tileTree, limit, cfg);
                }

                if (Objects.nonNull(tileTree)) {
                  int jobSize = getJobSize(tileTree);
                  TileMatrixPartitions tileMatrixPartitions =
                      tileMatrixPartitionsByJobSize.computeIfAbsent(
                          jobSize, TileMatrixPartitions::new);
                  List<Integer> progressLevels =
                      tileSetLevels
                          .get(tileSet)
                          .computeIfAbsent(
                              tileMatrixSet,
                              k ->
                                  IntStream.range(0, 24)
                                      .mapToObj(i -> -1)
                                      .collect(Collectors.toCollection(ArrayList::new)));

                  rootJobs.add(
                      tileTree.accept(
                          (tt, childSubtreeJobs) -> {
                            List<PartialJobConfiguration> followUps =
                                new ArrayList<>(childSubtreeJobs);

                            for (TileSubMatrix content : tt.getContent()) {
                              for (TileSubMatrix partition :
                                  tileMatrixPartitions.getSubMatrices(content.toLimits())) {
                                followUps.add(
                                    Tile3dSeedingPartialJob.content(
                                        job.priority(),
                                        jobContext,
                                        seedingJob.isReseed(),
                                        partition));
                                countTiles(partition, tileSet, tileSetTotals, progressLevels);
                              }
                            }

                            TileSubMatrix subtree = tt.toSubMatrix();
                            numSubtrees[0] += (int) subtree.getNumberOfTiles();
                            countTiles(subtree, tileSet, tileSetTotals, progressLevels);

                            return Tile3dSeedingPartialJob.subtree(
                                job.priority(),
                                jobContext,
                                seedingJob.isReseed(),
                                subtree,
                                followUps);
                          }));
                }
              });
        }

        int progressTotal = tileSetTotals.values().stream().mapToInt(Integer::intValue).sum();
        jobs.init(job.id(), progressTotal, progressDetails(tileSetTotals, tileSetLevels));
        rootJobs.forEach(jobs::push);

        if (LOGGER.isDebugEnabled() || LOGGER.isDebugEnabled(MARKER.JOBS)) {
          LOGGER.debug(
              MARKER.JOBS,
              "{}: processing {} subtrees and {} tiles with {} local processors",
              job.label(),
              numSubtrees[0],
              Math.max(progressTotal - numSubtrees[0], 0),
              concurrency);
        }
      } catch (IOException e) {
        return jobs.failure(e.getMessage());
      }
    }

    return jobs.success();
  }

  private static void countTiles(
      TileSubMatrix subMatrix,
      String tileSet,
      Map<String, Integer> tileSetTotals,
      List<Integer> progressLevels) {
    int count = (int) subMatrix.getNumberOfTiles();
    int old = progressLevels.get(subMatrix.getLevel());

    tileSetTotals.merge(tileSet, count, Integer::sum);
    progressLevels.set(subMatrix.getLevel(), old == -1 ? count : old + count);
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
    } else if (numberOfTiles <= 16_384) {
      return 64;
    }
    return 256;
  }

  private static TileTree mergeTileTree(
      TileTree currentTree, TileMatrixSetLimits limit, Tileset3dFeatures cfg) {
    TileTree next = TileTree.from(TileSubMatrix.of(limit), cfg.getSubtreeLevels());
    return currentTree == null ? next : currentTree.merge(next);
  }

  private static Tile3dSeedingPartialJob.Context createJobContext(
      Tile3dProviderFeatures tileProvider,
      Tile3dSeedingJob seedingJob,
      Job job,
      String tileSet,
      String tileMatrixSet) {
    return new Tile3dSeedingPartialJob.Context(
        tileProvider.getId(),
        tileSet,
        tileMatrixSet,
        Optional.of(seedingJob.getTileSetParameters().get(tileSet)),
        job.id());
  }

  private void cleanup(Job job, Tile3dProvider tileProvider, Tile3dSeedingJob seedingJob)
      throws IOException {
    tileProvider.seeding().get().cleanupSeeding(seedingJob);

    long duration = Instant.now().toEpochMilli() - job.startedAt();
    List<String> errors = job.errors();

    if (!errors.isEmpty() && (LOGGER.isWarnEnabled() || LOGGER.isWarnEnabled(MARKER.JOBS))) {
      LOGGER.warn(MARKER.JOBS, "{} had {} errors{}", job.label(), errors.size(), job.description());

      if (LOGGER.isDebugEnabled() || LOGGER.isDebugEnabled(MARKER.JOBS)) {
        for (String error : errors) {
          LOGGER.debug(MARKER.JOBS, "{} error: {}{}", job.label(), error, job.description());
        }
      }
    }

    if (LOGGER.isInfoEnabled() || LOGGER.isInfoEnabled(MARKER.JOBS)) {
      LOGGER.info(
          MARKER.JOBS,
          "{} finished in {}{}",
          job.label(),
          prettyDuration(duration),
          job.description());
    }
  }

  // Tileset progress must be fully initialized here, since the queue only increments existing
  // paths. Percent is left out because the queue cannot derive it per tileset.
  private static Map<String, Object> progressDetails(
      Map<String, Integer> tileSetTotals, Map<String, Map<String, List<Integer>>> tileSetLevels) {
    Map<String, Object> progressDetails = new LinkedHashMap<>();
    tileSetTotals.forEach(
        (tileSet, total) ->
            progressDetails.put(
                tileSet,
                Map.of("current", 0, "total", total, "levels", tileSetLevels.get(tileSet))));
    return progressDetails;
  }

  @Override
  public Class<Tile3dSeedingJob> getInputsClass() {
    return Tile3dSeedingJob.class;
  }

  @Override
  public Class<JobContextNone> getPartialContextClass() {
    return JobContextNone.class;
  }

  private Optional<Tile3dProviderFeatures> getTileProvider(String id) {
    return entityRegistry.getEntity(Tile3dProviderFeatures.class, id);
  }

  private static String prettyDuration(long millis) {
    Duration d = millis > 1000 ? Duration.ofSeconds(millis / 1000) : Duration.ofMillis(millis);
    return AmountFormats.wordBased(d, Locale.ENGLISH);
  }
}
