/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.tiles3d.domain.Tile3dJobProcessor;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJob;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingPartialJob;
import de.ii.xtraplatform.xtralink.domain.JobProcessorBase;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Set;
import java.util.function.Consumer;

@Singleton
@AutoBind(interfaces = JobProcessorBase.class)
public class Tile3dJobProcessorSubtree extends Tile3dJobProcessor {

  @Inject
  Tile3dJobProcessorSubtree(EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  public Set<String> getKinds() {
    return Set.of(Tile3dSeedingPartialJob.TYPE_SUBTREE);
  }

  @Override
  protected void executeJob(
      Tile3dProvider tileProvider,
      Tile3dSeedingPartialJob seedingPartialJob,
      Tile3dSeedingJob seedingJob,
      Consumer<Integer> updateProgress)
      throws IOException {
    tileProvider.seeding().get().seedSubtrees(seedingPartialJob, updateProgress);
  }
}
