/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.google.common.hash.Funnel;
import de.ii.xtraplatform.tiles.domain.JacksonXmlAnnotation.XmlIgnore;
import java.nio.charset.StandardCharsets;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import org.immutables.value.Value;

@Value.Immutable
public interface TileMatrixSetLimits {

  String XMLNS = "http://www.opengis.net/wmts/1.0";

  @SuppressWarnings("UnstableApiUsage")
  Funnel<TileMatrixSetLimits> FUNNEL =
      (from, into) -> {
        into.putString(from.getTileMatrix(), StandardCharsets.UTF_8);
        into.putInt(from.getMinTileRow());
        into.putInt(from.getMaxTileRow());
        into.putInt(from.getMinTileCol());
        into.putInt(from.getMaxTileCol());
      };

  @JacksonXmlProperty(namespace = XMLNS, localName = "TileMatrix")
  String getTileMatrix();

  @JacksonXmlProperty(namespace = XMLNS, localName = "MinTileRow")
  Integer getMinTileRow();

  @JacksonXmlProperty(namespace = XMLNS, localName = "MaxTileRow")
  Integer getMaxTileRow();

  @JacksonXmlProperty(namespace = XMLNS, localName = "MinTileCol")
  Integer getMinTileCol();

  @JacksonXmlProperty(namespace = XMLNS, localName = "MaxTileCol")
  Integer getMaxTileCol();

  default boolean contains(int row, int col) {
    return getMaxTileCol() >= col
        && getMinTileCol() <= col
        && getMaxTileRow() >= row
        && getMinTileRow() <= row;
  }

  @Value.Derived
  @Value.Auxiliary
  @XmlIgnore
  default long getNumberOfTiles() {
    return ((long) getMaxTileRow() - getMinTileRow() + 1) * (getMaxTileCol() - getMinTileCol() + 1);
  }

  default long getNumberOfTiles(IntPredicate whereColMatches) {
    long numCols =
        IntStream.rangeClosed(getMinTileCol(), getMaxTileCol()).filter(whereColMatches).count();

    return (getMaxTileRow() - getMinTileRow() + 1) * numCols;
  }
}
