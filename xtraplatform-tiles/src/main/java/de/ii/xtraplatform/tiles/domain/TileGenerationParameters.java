/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTileGenerationParameters.Builder.class)
public interface TileGenerationParameters extends GenerationParameters {

  @Override
  Optional<BoundingBox> getClipBoundingBox();

  Map<String, String> getSubstitutions();

  @JsonIgnore
  @Value.Derived
  default Optional<PropertyTransformations> getPropertyTransformations() {
    if (getSubstitutions().isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(((PropertyTransformations) Map::of).withSubstitutions(getSubstitutions()));
  }

  @JsonIgnore
  @Value.Derived
  default boolean isEmpty() {
    return getClipBoundingBox().isEmpty() && getPropertyTransformations().isEmpty();
  }
}
