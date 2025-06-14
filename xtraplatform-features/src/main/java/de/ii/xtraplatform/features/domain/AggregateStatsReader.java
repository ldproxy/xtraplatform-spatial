/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.streams.domain.Reactive.Stream;
import java.util.List;
import java.util.Optional;
import org.threeten.extra.Interval;

public interface AggregateStatsReader<T> {

  Stream<Long> getCount(List<T> sourceSchemas);

  Stream<Optional<BoundingBox>> getSpatialExtent(List<T> sourceSchemas, boolean is3d);

  Stream<Optional<Interval>> getTemporalExtent(List<T> sourceSchemas);
}
