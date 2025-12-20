/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonInclude(Include.NON_EMPTY)
@JsonDeserialize(builder = ImmutableTileMatrix.Builder.class)
public interface TileMatrix {

  int SIGNIFICANT_DIGITS = 15;

  String getId();

  Optional<String> getTitle();

  Optional<String> getDescription();

  List<String> getKeywords();

  long getTileWidth();

  long getTileHeight();

  long getMatrixWidth();

  long getMatrixHeight();

  BigDecimal getScaleDenominator();

  BigDecimal getCellSize();

  BigDecimal[] getPointOfOrigin();

  @Value.Default
  default String getCornerOfOrigin() {
    return "topLeft";
  }

  @JsonIgnore
  @Value.Derived
  default int getTileLevel() {
    return Integer.parseInt(getId());
  }
}
