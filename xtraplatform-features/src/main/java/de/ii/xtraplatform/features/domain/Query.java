/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureStream.PipelineSteps;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

public interface Query {

  Optional<EpsgCrs> getCrs();

  @Value.Default
  default double getMaxAllowableOffset() {
    return 0;
  }

  @Value.Default
  default List<Integer> getGeometryPrecision() {
    return ImmutableList.of(0, 0, 0);
  }

  @Value.Default
  default List<Integer> getWgs84GeometryPrecision() {
    return ImmutableList.of(0, 0, 0);
  }

  @Value.Default
  default int getLimit() {
    return 0;
  }

  @Value.Default
  default int getOffset() {
    return 0;
  }

  @Value.Default
  default boolean hitsOnly() {
    return false;
  }

  @Value.Default
  default List<PipelineSteps> debugSkipPipelineSteps() {
    return List.of();
  }
}
