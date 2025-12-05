/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain.spec;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.Funnel;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true)
@JsonDeserialize(builder = ImmutableSubtree.Builder.class)
public interface Subtree {

  Logger LOGGER = LoggerFactory.getLogger(Subtree.class);

  @SuppressWarnings("UnstableApiUsage")
  Funnel<Subtree> FUNNEL =
      (from, into) -> {
        from.getBuffers().forEach(v -> Buffer.FUNNEL.funnel(v, into));
        from.getBufferViews().forEach(v -> BufferView.FUNNEL.funnel(v, into));
        from.getPropertyTables().forEach(v -> PropertyTable.FUNNEL.funnel(v, into));
        Availability.FUNNEL.funnel(from.getTileAvailability(), into);
        from.getContentAvailability().forEach(v -> Availability.FUNNEL.funnel(v, into));
        Availability.FUNNEL.funnel(from.getChildSubtreeAvailability(), into);
        from.getTileMetadata().ifPresent(into::putInt);
        from.getContentMetadata().forEach(into::putInt);
        from.getSubtreeMetadata().ifPresent(v -> MetadataEntity.FUNNEL.funnel(v, into));
      };

  byte[] MAGIC_SUBT = {0x73, 0x75, 0x62, 0x74};
  byte[] VERSION_1 = {0x01, 0x00, 0x00, 0x00};
  byte[] JSON_PADDING = {0x20};
  byte[] BIN_PADDING = {0x00};
  byte[] EMPTY = new byte[0];

  List<Buffer> getBuffers();

  List<BufferView> getBufferViews();

  List<PropertyTable> getPropertyTables();

  Availability getTileAvailability();

  List<Availability> getContentAvailability();

  Availability getChildSubtreeAvailability();

  Optional<Integer> getTileMetadata();

  List<Integer> getContentMetadata();

  Optional<MetadataEntity> getSubtreeMetadata();

  @JsonIgnore
  @Value.Default
  default byte[] getTileAvailabilityBin() {
    return EMPTY;
  }

  @JsonIgnore
  @Value.Default
  default byte[] getContentAvailabilityBin() {
    return EMPTY;
  }

  @JsonIgnore
  @Value.Default
  default byte[] getChildSubtreeAvailabilityBin() {
    return EMPTY;
  }
}
