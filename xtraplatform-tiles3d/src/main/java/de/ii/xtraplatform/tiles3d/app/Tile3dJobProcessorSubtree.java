/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.tiles3d.domain.Tile3dJobProcessor;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJob;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJobSet;
import java.io.IOException;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
@AutoBind
public class Tile3dJobProcessorSubtree extends Tile3dJobProcessor {

  @Inject
  Tile3dJobProcessorSubtree(AppContext appContext, EntityRegistry entityRegistry) {
    super(appContext, entityRegistry);
  }

  @Override
  public String getJobType() {
    return Tile3dSeedingJob.TYPE_SUBTREE;
  }

  @Override
  protected void executeJob(
      String jobType,
      Tile3dProvider tileProvider,
      Tile3dSeedingJob seedingJob,
      Tile3dSeedingJobSet seedingJobSet,
      Consumer<Integer> updateProgress)
      throws IOException {
    tileProvider.seeding().get().seedSubtrees(seedingJob, updateProgress);
  }
}
