/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain.spec;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.hash.Funnel;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableBoundingVolume.Builder.class)
@JsonInclude(Include.NON_EMPTY)
public interface BoundingVolume {

  @SuppressWarnings("UnstableApiUsage")
  Funnel<BoundingVolume> FUNNEL =
      (from, into) -> {
        from.getRegion().forEach(into::putDouble);
      };

  List<Double> getRegion();

  List<Double> getBox();

  List<Double> getSphere();

  default Optional<BoundingBox> toBoundingBox() {
    List<Double> region = getRegion();
    if (region.size() != 6) {
      return Optional.empty();
    }
    BoundingBox bbox =
        BoundingBox.of(
            Math.toDegrees(region.get(0)),
            Math.toDegrees(region.get(1)),
            region.get(4),
            Math.toDegrees(region.get(2)),
            Math.toDegrees(region.get(3)),
            region.get(5),
            OgcCrs.CRS84h);
    return Optional.of(bbox);
  }
}
