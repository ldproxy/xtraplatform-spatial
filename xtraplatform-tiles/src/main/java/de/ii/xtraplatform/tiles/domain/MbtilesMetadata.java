/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.immutables.value.Value;

/** Mbtiles metadata */
@Value.Immutable
@Value.Style(deepImmutablesDetection = true)
@JsonDeserialize(builder = ImmutableMbtilesMetadata.Builder.class)
public abstract class MbtilesMetadata {

  public enum MbtilesType {
    OVERLAY,
    BASELAYER;

    public static MbtilesType of(String value) {
      if ("overlay".equals(value)) {
        return OVERLAY;
      }
      if ("baselayer".equals(value)) {
        return BASELAYER;
      }
      return null;
    }

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  public abstract String getName();

  public abstract TilesFormat getFormat();

  public abstract List<Double> getBounds();

  public abstract List<Number> getCenter();

  public abstract Optional<Integer> getMinzoom();

  public abstract Optional<Integer> getMaxzoom();

  public abstract Optional<String> getDescription();

  public abstract Optional<String> getAttribution();

  public abstract Optional<MbtilesType> getType();

  public abstract Optional<Number> getVersion();

  public abstract List<VectorLayer> getVectorLayers();

  // TODO support tilestats
  //      see https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#tilestats
}
