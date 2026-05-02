/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import de.ii.xtraplatform.tiles.domain.ChainedTileProvider;
import de.ii.xtraplatform.tiles.domain.TileEncoder;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetBase;
import de.ii.xtraplatform.tiles.domain.TileProviderFeaturesData;
import de.ii.xtraplatform.tiles.domain.TileQuery;
import de.ii.xtraplatform.tiles.domain.TileResult;
import java.io.IOException;
import java.util.Map;
import jakarta.ws.rs.core.MediaType;

public class TileEncoders implements ChainedTileProvider {
  private static final Map<MediaType, TileEncoder> ENCODERS =
      ImmutableMap.of(FeatureEncoderMVT.FORMAT, new TileEncoderMvt());
  private final TileProviderFeaturesData data;
  private final ChainedTileProvider generatorProviderChain;

  public TileEncoders(TileProviderFeaturesData data, ChainedTileProvider generatorProviderChain) {
    this.data = data;
    this.generatorProviderChain = generatorProviderChain;
  }

  @Override
  public Map<String, Map<String, Range<Integer>>> getTmsRanges() {
    return data.getTmsRanges();
  }

  @Override
  public boolean canProvide(TileQuery tile) {
    return ChainedTileProvider.super.canProvide(tile)
        && !data.getTilesets().get(tile.getTileset()).getCombine().isEmpty()
        && canEncode(tile.getMediaType());
  }

  @Override
  public TileResult getTile(TileQuery tile) throws IOException {
    return TileResult.found(combine(tile));
  }

  public boolean canEncode(MediaType mediaType) {
    return ENCODERS.containsKey(mediaType);
  }

  public byte[] empty(MediaType mediaType, TileMatrixSetBase tms) {
    return ENCODERS.get(mediaType).empty(tms);
  }

  public byte[] combine(TileQuery tile) throws IOException {
    return ENCODERS.get(tile.getMediaType()).combine(tile, data, generatorProviderChain);
  }
}
