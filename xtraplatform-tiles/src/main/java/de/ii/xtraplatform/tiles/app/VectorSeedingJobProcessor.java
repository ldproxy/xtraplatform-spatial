/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.base.domain.LogContext.MARKER;
import de.ii.xtraplatform.base.domain.resiliency.Volatile2.State;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.jobs.domain.Job;
import de.ii.xtraplatform.jobs.domain.JobProcessor;
import de.ii.xtraplatform.jobs.domain.JobQueueMin;
import de.ii.xtraplatform.jobs.domain.JobResult;
import de.ii.xtraplatform.jobs.domain.JobSet;
import de.ii.xtraplatform.tiles.domain.TileProvider;
import de.ii.xtraplatform.tiles.domain.TileSeedingJob;
import de.ii.xtraplatform.tiles.domain.TileSeedingJobSet;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class VectorSeedingJobProcessor implements JobProcessor<TileSeedingJob, TileSeedingJobSet> {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorSeedingJobProcessor.class);

  private final int concurrency;
  private final EntityRegistry entityRegistry;

  @Inject
  VectorSeedingJobProcessor(AppContext appContext, EntityRegistry entityRegistry) {
    this.concurrency = appContext.getConfiguration().getJobConcurrency();
    this.entityRegistry = entityRegistry;
  }

  @Override
  public String getJobType() {
    return TileSeedingJob.TYPE_MVT;
  }

  @Override
  public int getPriority() {
    return 1000;
  }

  @Override
  public int getConcurrency(JobSet jobSet) {
    return concurrency;
  }

  @Override
  public JobResult process(Job job, JobSet jobSet, JobQueueMin jobQueue) {
    TileSeedingJob seedingJob = getDetails(job, jobQueue);
    TileSeedingJobSet seedingJobSet = getSetDetails(jobSet, jobQueue);

    Optional<TileProvider> optionalTileProvider = getTileProvider(seedingJob.getTileProvider());
    if (optionalTileProvider.isPresent()) {
      TileProvider tileProvider = optionalTileProvider.get();

      if (!tileProvider.seeding().isSupported()) {
        LOGGER.error("Tile provider does not support seeding: {}", tileProvider.getId());
        return JobResult.error("Tile provider does not support seeding"); // early return
      }
      if (!tileProvider.seeding().isAvailable()) {
        if (LOGGER.isDebugEnabled(MARKER.JOBS) || LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              MARKER.JOBS,
              "Tile provider '{}' not available, suspending job ({})",
              tileProvider.getId(),
              job.getId());
        }
        tileProvider
            .seeding()
            .onStateChange(
                (oldState, newState) -> {
                  if (newState == State.AVAILABLE) {
                    if (LOGGER.isDebugEnabled(MARKER.JOBS) || LOGGER.isTraceEnabled()) {
                      LOGGER.trace(
                          MARKER.JOBS,
                          "Tile provider '{}' became available, resuming job ({})",
                          tileProvider.getId(),
                          job.getId());
                    }
                    jobQueue.push(job);
                  }
                },
                true);
        return JobResult.onHold(); // early return
      }

      AtomicInteger last = new AtomicInteger(0);
      Consumer<Integer> updateProgress =
          (current) -> {
            int delta = current - last.getAndSet(current);
            Map<String, Object> detailParameters =
                Map.of(
                    "tileSet", seedingJob.getTileSet(),
                    "tileMatrixSet", seedingJob.getTileMatrixSet(),
                    "level", seedingJob.getSubMatrices().get(0).getLevel(),
                    "delta", delta);

            jobQueue.updateJob(job, delta);
            jobQueue.updateJobSet(jobSet, delta, detailParameters);
          };

      try {
        tileProvider.seeding().get().runSeeding(seedingJob, updateProgress);
      } catch (IOException e) {
        return JobResult.retry(e.getMessage());
      } catch (Throwable e) {
        updateProgress.accept(job.getTotal().get());
        throw e;
      }
    }

    return JobResult.success();
  }

  @Override
  public Class<TileSeedingJob> getDetailsType() {
    return TileSeedingJob.class;
  }

  @Override
  public Class<TileSeedingJobSet> getSetDetailsType() {
    return TileSeedingJobSet.class;
  }

  @Override
  public Map<String, Class<?>> getJobTypes() {
    return Map.of(
        TileSeedingJob.TYPE_MVT,
        TileSeedingJob.class,
        TileSeedingJob.TYPE_PNG,
        TileSeedingJob.class);
  }

  private Optional<TileProvider> getTileProvider(String id) {
    return entityRegistry.getEntity(TileProvider.class, id);
  }
}
