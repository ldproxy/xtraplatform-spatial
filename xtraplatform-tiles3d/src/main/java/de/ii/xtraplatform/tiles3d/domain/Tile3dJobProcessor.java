/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import de.ii.xtralink.jobs.Job;
import de.ii.xtralink.jobs.JobResult;
import de.ii.xtralink.jobs.PartialJob;
import de.ii.xtraplatform.base.domain.LogContext.MARKER;
import de.ii.xtraplatform.base.domain.resiliency.Volatile2.State;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.tiles3d.app.Tile3dProviderFeatures;
import de.ii.xtraplatform.xtralink.domain.JobProcessing;
import de.ii.xtraplatform.xtralink.domain.JobProcessor;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Tile3dJobProcessor
    implements JobProcessor<Tile3dSeedingJob, Tile3dSeedingPartialJob> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dJobProcessor.class);

  private final EntityRegistry entityRegistry;

  protected Tile3dJobProcessor(EntityRegistry entityRegistry) {
    this.entityRegistry = entityRegistry;
  }

  @Override
  public int getPriority() {
    return 1000;
  }

  protected abstract void executeJob(
      Tile3dProvider tileProvider,
      Tile3dSeedingPartialJob seedingPartialJob,
      Tile3dSeedingJob seedingJob,
      Consumer<Integer> updateProgress)
      throws IOException;

  @Override
  public JobResult process(PartialJob partialJob, Job job, JobProcessing jobs) throws Exception {
    Tile3dSeedingPartialJob seedingPartialJob = getPartialContext(partialJob, jobs);
    Tile3dSeedingJob seedingJob = getInputs(job, jobs);

    Optional<Tile3dProviderFeatures> optionalTileProvider =
        getTileProvider(seedingPartialJob.getTileProvider());
    if (optionalTileProvider.isPresent()) {
      Tile3dProviderFeatures tileProvider = optionalTileProvider.get();

      if (!tileProvider.seeding().isSupported()) {
        LOGGER.error("Tile provider does not support seeding: {}", tileProvider.getId());
        return jobs.failure("Tile provider does not support seeding"); // early return
      }
      if (!tileProvider.seeding().isAvailable()) {
        suspendUntilAvailable(tileProvider, partialJob, jobs);
        return jobs.onHold(); // early return
      }

      AtomicInteger last = new AtomicInteger(0);
      Consumer<Integer> updateProgress =
          (current) -> {
            int delta = current - last.getAndSet(current);
            jobs.update(partialJob.id(), delta);
          };

      try {
        executeJob(tileProvider, seedingPartialJob, seedingJob, updateProgress);
      } catch (IOException e) {
        return jobs.retry(e.getMessage());
      } catch (Throwable e) { // NOPMD AvoidCatchingThrowable
        updateProgress.accept(partialJob.progress().total());
        throw e;
      }

      seedingPartialJob.getFollowUps().forEach(jobs::push);
    }

    return jobs.success();
  }

  @Override
  public Class<Tile3dSeedingJob> getInputsClass() {
    return Tile3dSeedingJob.class;
  }

  @Override
  public Class<Tile3dSeedingPartialJob> getPartialContextClass() {
    return Tile3dSeedingPartialJob.class;
  }

  private Optional<Tile3dProviderFeatures> getTileProvider(String id) {
    return entityRegistry.getEntity(Tile3dProviderFeatures.class, id);
  }

  private void suspendUntilAvailable(
      Tile3dProviderFeatures tileProvider, PartialJob partialJob, JobProcessing jobs) {
    if (LOGGER.isTraceEnabled(MARKER.JOBS)) {
      LOGGER.trace(
          MARKER.JOBS,
          "Tile provider '{}' not available, suspending job ({})",
          tileProvider.getId(),
          partialJob.id());
    }

    tileProvider
        .seeding()
        .onStateChange(
            (oldState, newState) -> {
              if (newState != State.AVAILABLE) {
                return;
              }

              if (LOGGER.isTraceEnabled(MARKER.JOBS)) {
                LOGGER.trace(
                    MARKER.JOBS,
                    "Tile provider '{}' became available, resuming job ({})",
                    tileProvider.getId(),
                    partialJob.id());
              }
              jobs.repush(partialJob.id());
            },
            true);
  }
}
