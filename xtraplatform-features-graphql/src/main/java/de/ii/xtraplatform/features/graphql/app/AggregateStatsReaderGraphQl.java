/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.graphql.app;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.features.domain.AggregateStatsReader;
import de.ii.xtraplatform.features.domain.FeatureMetadata;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Stream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.threeten.extra.Interval;

public class AggregateStatsReaderGraphQl implements AggregateStatsReader<FeatureSchema> {

  public AggregateStatsReaderGraphQl(
      FeatureMetadata featureMetadata,
      CrsTransformerFactory crsTransformerFactory,
      EpsgCrs nativeCrs) {
    Objects.requireNonNull(featureMetadata);
    Objects.requireNonNull(crsTransformerFactory)
        .getTransformer(OgcCrs.CRS84, Objects.requireNonNull(nativeCrs), true);
  }

  @Override
  public Stream<Long> getCount(List<FeatureSchema> sourceSchemas) {
    // TODO: hits query //NOPMD ForbiddenContent
    return Reactive.Source.single(-1L).to(Reactive.Sink.head());
  }

  @Override
  public Stream<Optional<BoundingBox>> getSpatialExtent(
      List<FeatureSchema> sourceSchemas, boolean is3d) {

    Optional<BoundingBox> boundingBox = Optional.empty();

    return Reactive.Source.single(boundingBox).to(Reactive.Sink.head());
  }

  @Override
  public Stream<Optional<Interval>> getTemporalExtent(List<FeatureSchema> sourceSchemas) {
    return Reactive.Source.single(Optional.<Interval>empty()).to(Reactive.Sink.head());
  }
}
