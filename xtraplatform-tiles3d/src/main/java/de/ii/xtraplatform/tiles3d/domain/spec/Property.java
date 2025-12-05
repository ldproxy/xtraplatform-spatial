/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain.spec;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.Funnel;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true)
@JsonDeserialize(builder = ImmutableProperty.Builder.class)
public interface Property {

  @SuppressWarnings("UnstableApiUsage")
  Funnel<Property> FUNNEL =
      (from, into) -> {
        into.putInt(from.getValues());
        from.getArrayOffsets().ifPresent(into::putInt);
        from.getStringOffsets().ifPresent(into::putInt);
        from.getArrayOffsetType().ifPresent(v -> into.putString(v.name(), StandardCharsets.UTF_8));
        from.getStringOffsetType().ifPresent(v -> into.putString(v.name(), StandardCharsets.UTF_8));
        from.getOffset().ifPresent(n -> into.putDouble((double) n));
        from.getScale().ifPresent(n -> into.putDouble((double) n));
        from.getMin().ifPresent(n -> into.putDouble((double) n));
        from.getMax().ifPresent(n -> into.putDouble((double) n));
      };

  enum OffsetType {
    UINT8,
    UINT16,
    UINT32,
    UINT64,
  }

  int getValues();

  Optional<Integer> getArrayOffsets();

  Optional<Integer> getStringOffsets();

  Optional<OffsetType> getArrayOffsetType();

  Optional<OffsetType> getStringOffsetType();

  Optional<Number> getOffset();

  Optional<Number> getScale();

  Optional<Number> getMin();

  Optional<Number> getMax();
}
