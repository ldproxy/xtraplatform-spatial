/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.base.domain.LogContext.MARKER;
import de.ii.xtraplatform.base.domain.resiliency.Volatile2.State;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.jobs.domain.Job;
import de.ii.xtraplatform.jobs.domain.JobProcessor;
import de.ii.xtraplatform.jobs.domain.JobResult;
import de.ii.xtraplatform.jobs.domain.JobSet;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJob;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJobSet;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Tile3dJobProcessor
    implements JobProcessor<Tile3dSeedingJob, Tile3dSeedingJobSet> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dJobProcessor.class);

  private final int concurrency;
  private final EntityRegistry entityRegistry;

  Tile3dJobProcessor(AppContext appContext, EntityRegistry entityRegistry) {
    this.concurrency = appContext.getConfiguration().getBackgroundTasks().getMaxThreads();
    this.entityRegistry = entityRegistry;
  }

  @Override
  public int getPriority() {
    return 1000;
  }

  @Override
  public int getConcurrency(JobSet jobSet) {
    return concurrency;
  }

  protected abstract void executeJob(
      String jobType,
      Tile3dProvider tileProvider,
      Tile3dSeedingJob seedingJob,
      Tile3dSeedingJobSet seedingJobSet,
      Consumer<Integer> updateProgress)
      throws IOException;

  @Override
  public JobResult process(Job job, JobSet jobSet, Consumer<Job> pushJob) {
    Tile3dSeedingJob seedingJob = getDetails(job);
    Tile3dSeedingJobSet seedingJobSet = getSetDetails(jobSet);

    Optional<Tile3dProvider> optionalTileProvider = getTileProvider(seedingJob.getTileProvider());
    if (optionalTileProvider.isPresent()) {
      Tile3dProvider tileProvider = optionalTileProvider.get();

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
                    pushJob.accept(job);
                  }
                },
                true);
        return JobResult.onHold(); // early return
      }

      AtomicInteger last = new AtomicInteger(0);
      Consumer<Integer> updateProgress =
          (current) -> {
            int delta = current - last.getAndSet(current);

            job.update(delta);
            jobSet.update(delta);
            seedingJobSet.update(
                seedingJob.getTileSet(),
                seedingJob.getTileMatrixSet(),
                seedingJob.getSubMatrices().get(0).getLevel(),
                delta);
          };

      try {
        executeJob(job.getType(), tileProvider, seedingJob, seedingJobSet, updateProgress);
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
  public Class<Tile3dSeedingJob> getDetailsType() {
    return Tile3dSeedingJob.class;
  }

  @Override
  public Class<Tile3dSeedingJobSet> getSetDetailsType() {
    return Tile3dSeedingJobSet.class;
  }

  private Optional<Tile3dProvider> getTileProvider(String id) {
    return entityRegistry.getEntity(Tile3dProvider.class, id);
  }
}
