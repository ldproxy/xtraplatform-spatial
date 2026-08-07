/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface WithCenter {

  /**
   * @langEn Longitude and latitude that a map with the tiles should be centered on by default.
   * @langDe Legt Länge und Breite fest, auf die standardmäßig eine Karte mit den Kacheln zentriert
   *     werden sollte.
   * @default { lon: 0.0, lat: 0.0 }
   * @since v3.4
   */
  Optional<LonLat> getCenter();

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableLonLat.Builder.class)
  interface LonLat {

    static LonLat of(double lon, double lat) {
      return new ImmutableLonLat.Builder().lon(lon).lat(lat).build();
    }

    double getLon();

    double getLat();

    @JsonIgnore
    @Value.Derived
    default List<Double> asList() {
      return ImmutableList.of(getLon(), getLat());
    }
  }
}
