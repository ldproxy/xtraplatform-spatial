/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Range;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(builder = "new")
@JsonDeserialize(builder = ImmutableMinMax.Builder.class)
public interface MinMax extends Buildable<MinMax> {

  static MinMax of(Range<Integer> range) {
    return new ImmutableMinMax.Builder()
        .min(range.lowerEndpoint())
        .max(range.upperEndpoint())
        .build();
  }

  static MinMax of(int min, int max) {
    return new ImmutableMinMax.Builder().min(min).max(max).build();
  }

  int getMin();

  int getMax();

  Optional<Integer> getDefault();

  @Override
  default ImmutableMinMax.Builder getBuilder() {
    return new ImmutableMinMax.Builder().from(this);
  }

  abstract class Builder implements BuildableBuilder<MinMax> {}

  @Value.Lazy
  default Range<Integer> asRange() {
    return Range.closed(getMin(), getMax());
  }
}
