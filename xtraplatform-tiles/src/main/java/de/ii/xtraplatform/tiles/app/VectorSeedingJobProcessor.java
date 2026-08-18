/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtralink.jobs.JobResult;
import de.ii.xtralink.jobs.PartialJob;
import de.ii.xtraplatform.base.domain.LogContext.MARKER;
import de.ii.xtraplatform.base.domain.resiliency.Volatile2.State;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.tiles.domain.TileProvider;
import de.ii.xtraplatform.tiles.domain.TileSeedingJob;
import de.ii.xtraplatform.tiles.domain.TileSeedingPartialJob;
import de.ii.xtraplatform.xtralink.domain.JobProcessing;
import de.ii.xtraplatform.xtralink.domain.JobProcessor;
import de.ii.xtraplatform.xtralink.domain.JobProcessorBase;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind(interfaces = JobProcessorBase.class)
public class VectorSeedingJobProcessor
    implements JobProcessor<TileSeedingJob, TileSeedingPartialJob> {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorSeedingJobProcessor.class);

  private final EntityRegistry entityRegistry;

  @Inject
  VectorSeedingJobProcessor(EntityRegistry entityRegistry) {
    this.entityRegistry = entityRegistry;
  }

  @Override
  public Set<String> getKinds() {
    return Set.of(TileSeedingPartialJob.TYPE_MVT);
  }

  @Override
  public int getPriority() {
    return 1000;
  }

  @Override
  public JobResult process(PartialJob job, de.ii.xtralink.jobs.Job jobSet, JobProcessing jobs)
      throws Exception {
    TileSeedingPartialJob seedingJob = getPartialContext(job, jobs);

    Optional<TileProvider> optionalTileProvider = getTileProvider(seedingJob.getTileProvider());
    if (optionalTileProvider.isPresent()) {
      TileProvider tileProvider = optionalTileProvider.get();

      if (!tileProvider.seeding().isSupported()) {
        LOGGER.error("Tile provider does not support seeding: {}", tileProvider.getId());
        return jobs.failure("Tile provider does not support seeding"); // early return
      }
      if (!tileProvider.seeding().isAvailable()) {
        if (LOGGER.isDebugEnabled(MARKER.JOBS) || LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              MARKER.JOBS,
              "Tile provider '{}' not available, suspending job ({})",
              tileProvider.getId(),
              job.id());
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
                          job.id());
                    }
                    jobs.repush(job.id());
                  }
                },
                true);
        return jobs.onHold(); // early return
      }

      AtomicInteger last = new AtomicInteger(0);
      Consumer<Integer> updateProgress =
          (current) -> {
            int delta = current - last.getAndSet(current);
            jobs.update(job.id(), delta);
          };

      try {
        tileProvider.seeding().get().runSeeding(seedingJob, updateProgress);
      } catch (IOException e) {
        return jobs.retry(e.getMessage());
      } catch (Throwable e) {
        updateProgress.accept(job.progress().total());
        throw e;
      }
    }

    return jobs.success();
  }

  @Override
  public Class<TileSeedingJob> getInputsClass() {
    return TileSeedingJob.class;
  }

  @Override
  public Class<TileSeedingPartialJob> getPartialContextClass() {
    return TileSeedingPartialJob.class;
  }

  private Optional<TileProvider> getTileProvider(String id) {
    return entityRegistry.getEntity(TileProvider.class, id);
  }
}
