/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureStream.ResultBase;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

public interface FeatureTransactions {
  String PATCH_NULL_VALUE = "###NULL###";
  String CAPABILITY = "mutations";
  String LABEL = "Mutate features";

  @Value.Immutable
  interface MutationResult extends FeatureStream.ResultBase {

    enum Type {
      CREATE,
      REPLACE,
      UPDATE,
      DELETE
    }

    abstract class Builder extends ResultBase.Builder<MutationResult, MutationResult.Builder> {
      public abstract Builder addIds(String... ids);
    }

    Type getType();

    List<String> getIds();

    Optional<BoundingBox> getSpatialExtent();

    Optional<Tuple<Long, Long>> getTemporalExtent();

    @Value.Default
    @Override
    default boolean isEmpty() {
      return getIds().isEmpty();
    }
  }

  MutationResult createFeatures(
      String featureType, FeatureTokenSource featureTokenSource, EpsgCrs crs);

  MutationResult updateFeature(
      String type, String id, FeatureTokenSource featureTokenSource, EpsgCrs crs, boolean partial);

  MutationResult deleteFeature(String featureType, String id);
}
