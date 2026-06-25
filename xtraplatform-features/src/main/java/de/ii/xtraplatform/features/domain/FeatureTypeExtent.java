/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Optional;
import org.immutables.value.Value;

/** Extent object for spatial and temporal extents. */
@Value.Immutable
@JsonDeserialize(builder = ImmutableFeatureTypeExtent.Builder.class)
public interface FeatureTypeExtent {
  Optional<SpatialExtent> getSpatial();

  Optional<TemporalExtent> getTemporal();
}
