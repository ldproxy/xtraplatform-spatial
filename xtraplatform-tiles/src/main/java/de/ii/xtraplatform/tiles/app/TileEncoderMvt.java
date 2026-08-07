/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import de.ii.xtraplatform.tiles.domain.ChainedTileProvider;
import de.ii.xtraplatform.tiles.domain.ImmutableTileQuery;
import de.ii.xtraplatform.tiles.domain.TileEncoder;
import de.ii.xtraplatform.tiles.domain.TileGenerationParametersTransient;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetBase;
import de.ii.xtraplatform.tiles.domain.TileProviderFeaturesData;
import de.ii.xtraplatform.tiles.domain.TileQuery;
import de.ii.xtraplatform.tiles.domain.TileResult;
import de.ii.xtraplatform.tiles.domain.TilesetFeatures;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TileEncoderMvt implements TileEncoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileEncoderMvt.class);

  public TileEncoderMvt() {}

  @Override
  public byte[] empty(TileMatrixSetBase tms) {
    return new VectorTileEncoder(tms.getTileExtent()).encode();
  }

  @Override
  @SuppressWarnings("PMD.DoNotUseThreads")
  public byte[] combine(
      TileQuery tile, TileProviderFeaturesData data, ChainedTileProvider tileProvider)
      throws IOException {
    TilesetFeatures combinedTileset = data.getTilesets().get(tile.getTileset());
    List<String> tilesets =
        getLayerTilesets(data, combinedTileset, tile.getGenerationParametersTransient());
    VectorTileEncoder encoder = new VectorTileEncoder(tile.getTileMatrixSet().getTileExtent());
    VectorTileDecoder decoder = new VectorTileDecoder();

    for (String tileset : tilesets) {
      TileQuery tileQuery = ImmutableTileQuery.builder().from(tile).tileset(tileset).build();
      TileResult layer = tileProvider.get(tileQuery);

      int count = 0;
      while (layer.isError() && count < 3) {
        count++;
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignore) {
          Thread.currentThread().interrupt();
        }
        layer = tileProvider.get(tileQuery);
      }

      if (layer.isError()) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Failure to get layer '{}' of combined vector tile {}/{}/{}/{} (format '{}'), the layer will be ignored. Reason: {}",
              tileset,
              tileQuery.getTileMatrixSet().getId(),
              tileQuery.getLevel(),
              tileQuery.getRow(),
              tileQuery.getCol(),
              tileQuery.getMediaType().toString(),
              layer.getError().orElse("unknown"));
        }
      } else if (layer.isAvailable()) {
        decoder
            .decode(layer.getContent().get())
            .forEach(
                feature ->
                    encoder.addFeature(
                        feature.getLayerName(),
                        feature.getAttributes(),
                        feature.getGeometry(),
                        feature.getId()));
      }
    }

    return encoder.encode();
  }

  private List<String> getLayerTilesets(
      TileProviderFeaturesData data,
      TilesetFeatures combinedTileset,
      Optional<TileGenerationParametersTransient> userParameters) {
    return combinedTileset.getCombine().stream()
        .flatMap(
            layer -> {
              if (Objects.equals(layer, TilesetFeatures.COMBINE_ALL)) {
                return data.getTilesets().entrySet().stream()
                    .filter(entry -> !entry.getValue().isCombined())
                    .map(Entry::getKey);
              }
              return Stream.of(layer);
            })
        .filter(
            layer ->
                userParameters.isEmpty()
                    || userParameters.get().getLayers().isEmpty()
                    || userParameters.get().getLayers().contains(layer))
        .distinct()
        .collect(Collectors.toList());
  }
}
