/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.app;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.features.domain.AggregateStatsReader;
import de.ii.xtraplatform.features.domain.FeatureMetadata;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.Metadata;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Stream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.threeten.extra.Interval;

public class AggregateStatsReaderWfs implements AggregateStatsReader<FeatureSchema> {

  private final FeatureMetadata featureMetadata;

  public AggregateStatsReaderWfs(FeatureMetadata featureMetadata) {
    this.featureMetadata = featureMetadata;
  }

  @Override
  public Stream<Long> getCount(List<FeatureSchema> sourceSchemas) {
    return Reactive.Source.single(-1L).to(Reactive.Sink.head());
  }

  /** The bounding box from the capabilities, in CRS84. */
  @Override
  public Stream<Optional<BoundingBox>> getSpatialExtent(
      List<FeatureSchema> sourceSchemas, boolean is3d) {

    Optional<BoundingBox> boundingBox =
        featureMetadata
            .getMetadata()
            .map(Metadata::getFeatureTypesBoundingBox)
            .flatMap(
                boundingBoxes ->
                    getTypeIds(sourceSchemas.get(0)).stream()
                        .map(boundingBoxes::get)
                        .filter(Objects::nonNull)
                        .findFirst());

    return Reactive.Source.single(boundingBox).to(Reactive.Sink.head());
  }

  @Override
  public Stream<Optional<Interval>> getTemporalExtent(List<FeatureSchema> sourceSchemas) {
    return Reactive.Source.single(Optional.<Interval>empty()).to(Reactive.Sink.head());
  }

  /**
   * The ids under which the capabilities analyzer stores the bounding box of a feature type: the
   * prefixed name becomes {@code prefix_localname}, and if unique also {@code localname}, both in
   * lower case.
   */
  static List<String> getTypeIds(FeatureSchema schema) {
    String prefixedName =
        schema.getSourcePath().map(path -> path.substring(1)).orElse(schema.getName());
    int separator = prefixedName.indexOf(':');
    String localName = prefixedName.substring(separator + 1).toLowerCase(Locale.ROOT);

    List<String> ids = new ArrayList<>();
    ids.add(schema.getName());
    if (separator > 0) {
      ids.add(prefixedName.substring(0, separator).toLowerCase(Locale.ROOT) + "_" + localName);
    }
    ids.add(localName);
    return ids;
  }
}
