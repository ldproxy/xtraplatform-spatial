/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.CrsTransformationException;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.OgcCrs;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;

public class TileEncoderMvt implements TileEncoder {

  private final Function<String, Optional<BoundingBox>> tilesetBounds;
  private final CrsTransformerFactory crsTransformerFactory;

  private final Function<String, Optional<BoundingBox>> tilesetBounds;
  private final CrsTransformerFactory crsTransformerFactory;

  public TileEncoderMvt(
      Function<String, Optional<BoundingBox>> tilesetBounds,
      CrsTransformerFactory crsTransformerFactory) {
    this.tilesetBounds = tilesetBounds;
    this.crsTransformerFactory = crsTransformerFactory;
  }

  @Override
  public byte[] empty(TileMatrixSetBase tms) {
    return new VectorTileEncoder(tms.getTileExtent()).encode();
  }

  @Override
  public byte[] combine(
      TileQuery tile, TileProviderFeaturesData data, ChainedTileProvider tileProvider)
      throws IOException {
    TilesetFeatures combinedTileset =
        data.getTilesets().get(tile.getTileset()).mergeDefaults(data.getTilesetDefaults());
    List<String> tilesets =
        getLayerTilesets(data, combinedTileset, tile.getGenerationParametersTransient());
    // the layers of the source tilesets are decoded to a normalized coordinate space, so a source
    // tileset with a different tile extent is rescaled to the tile extent of the combined tileset
    VectorTileEncoder encoder =
        new VectorTileEncoder(combinedTileset.getTileExtentOrDefault(tile.getTileMatrixSet()));
    VectorTileDecoder decoder = new VectorTileDecoder();
    Optional<BoundingBox> tileBounds = getTileBounds(tile);

    for (String tileset : tilesets) {
      // a tileset without data in the area of the tile cannot contribute a layer; without this
      // check, a tile that is not in the cache, because it was not seeded, would be generated
      if (isOutsideBounds(tileset, tileBounds)) {
        continue;
      }

      TileQuery tileQuery = ImmutableTileQuery.builder().from(tile).tileset(tileset).build();
      TileResult layer = tileProvider.get(tileQuery);

      int count = 1;
      while (layer.isError() && count++ < 3) {
        try {
          Thread.sleep(100);
        } catch (Throwable ignore) {
        }
        layer = tileProvider.get(tileQuery);
      }

      if (layer.isError()) {
        // failing to get a layer has to fail the combined tile: a tile with a silently
        // dropped layer would be cached and served as if it were complete
        throw new IllegalStateException(
            String.format(
                "Failure to get layer '%s' of combined vector tile %s/%s/%s/%s (format '%s')."
                    + " Reason: %s",
                tileset,
                tileQuery.getTileMatrixSet().getId(),
                tileQuery.getLevel(),
                tileQuery.getRow(),
                tileQuery.getCol(),
                tileQuery.getMediaType().toString(),
                layer.getError().orElse("unknown")));
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

  private Optional<BoundingBox> getTileBounds(TileQuery tile) {
    try {
      return Optional.of(tile.getBoundingBox(OgcCrs.CRS84, crsTransformerFactory));
    } catch (CrsTransformationException e) {
      // ignore, assume that all tilesets may have data
      return Optional.empty();
    }
  }

  private boolean isOutsideBounds(String tileset, Optional<BoundingBox> tileBounds) {
    if (tileBounds.isEmpty()) {
      return false;
    }

    return tilesetBounds
        .apply(tileset)
        .filter(bounds -> !BoundingBox.intersects(bounds, tileBounds.get()))
        .isPresent();
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
