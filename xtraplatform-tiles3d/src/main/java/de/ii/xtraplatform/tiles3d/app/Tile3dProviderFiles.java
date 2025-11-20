/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Range;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedInject;
import de.ii.xtraplatform.base.domain.Jackson;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.entities.domain.Entity;
import de.ii.xtraplatform.entities.domain.Entity.SubType;
import de.ii.xtraplatform.features.domain.ProviderData;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetRepository;
import de.ii.xtraplatform.tiles.domain.TileResult;
import de.ii.xtraplatform.tiles3d.domain.Tile3dAccess;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderFilesData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dQuery;
import de.ii.xtraplatform.tiles3d.domain.Tile3dStoreReadOnly;
import de.ii.xtraplatform.tiles3d.domain.Tileset3dFiles;
import de.ii.xtraplatform.tiles3d.domain.spec.Tile3d;
import de.ii.xtraplatform.tiles3d.domain.spec.Tileset3d;
import de.ii.xtraplatform.tiles3d.domain.spec.WithUri;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Entity(
    type = Tile3dProviderData.ENTITY_TYPE,
    subTypes = {
      @SubType(key = ProviderData.PROVIDER_TYPE_KEY, value = Tile3dProviderData.PROVIDER_TYPE),
      @SubType(
          key = ProviderData.PROVIDER_SUB_TYPE_KEY,
          value = Tile3dProviderFilesData.PROVIDER_SUBTYPE)
    },
    data = Tile3dProviderFilesData.class)
public class Tile3dProviderFiles extends AbstractTile3dProvider<Tile3dProviderFilesData>
    implements Tile3dProvider, Tile3dAccess {

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dProviderFiles.class);

  private final ResourceStore rootStore;
  private final TileMatrixSetRepository tileMatrixSetRepository;
  private final VolatileRegistry volatileRegistry;
  private final Map<String, Tileset3d> metadata;
  private final Map<String, Tile3dStoreReadOnly> stores;
  private final Map<String, Map<String, Range<Integer>>> tmsRanges;
  private final ObjectMapper objectMapper;

  // private ChainedTile3dProvider providerChain;

  @AssistedInject
  public Tile3dProviderFiles(
      ResourceStore blobStore,
      TileMatrixSetRepository tileMatrixSetRepository,
      VolatileRegistry volatileRegistry,
      Jackson jackson,
      @Assisted Tile3dProviderFilesData data) {
    super(volatileRegistry, data, "access");

    this.rootStore = blobStore.with("3dtiles" /*TODO TileProviderFeatures.TILES_DIR_NAME*/);
    this.tileMatrixSetRepository = tileMatrixSetRepository;
    this.volatileRegistry = volatileRegistry;
    this.objectMapper = jackson.getDefaultObjectMapper();
    this.metadata = new LinkedHashMap<>();
    this.stores = new LinkedHashMap<>();
    this.tmsRanges = new LinkedHashMap<>();
  }

  @Override
  protected boolean onStartup() throws InterruptedException {
    onVolatileStart();

    addSubcomponent(rootStore, "access");
    addSubcomponent(tileMatrixSetRepository, "access");

    volatileRegistry.onAvailable(rootStore).toCompletableFuture().join();

    for (Entry<String, Tileset3dFiles> entry : getData().getTilesets().entrySet()) {
      Tileset3dFiles tilesetCfg = entry.getValue().mergeDefaults(getData().getTilesetDefaults());
      Path source = Path.of(tilesetCfg.getSource());

      try {
        if (!rootStore.has(source)) {
          throw new IllegalStateException("Could not find 3D Tiles tileset file: " + source);
        }

        InputStream inputStream = rootStore.content(source).orElseThrow();
        Tileset3d tileset = objectMapper.readValue(inputStream, Tileset3d.class);

        metadata.put(entry.getKey(), tileset);

        Tile3dStoreReadOnly store =
            tileset.getRoot().getImplicitTiling().isPresent()
                ? loadStoreImplicit(tileset, source)
                : loadStoreExplicit(tileset, source);

        stores.put(entry.getKey(), store);

      } catch (IOException e) {
        throw new IllegalStateException("Could not load 3D Tiles tileset file: " + source, e);
      }

      // String tms = "default"; // tileset.getTileMatrixSet();

      // return new SimpleImmutableEntry<>(toTilesetKey(entry.getKey(), tms), source);
    }

    // Tile3dStoreReadOnly tileStore = null; // TODO TileStoreMbTiles.readOnly(tilesetSources);

    /*this.providerChain =
    new ChainedTile3dProvider() {
      @Override
      public Map<String, Map<String, Range<Integer>>> getTmsRanges() {
        return tmsRanges;
      }

      @Override
      public TileResult getTile(Tile3dQuery tile) throws IOException {
        return tileStore.get(tile);
      }
    };*/

    // loadMetadata(tilesetSources);

    return true;
  }

  private Tile3dStoreReadOnly loadStoreImplicit(Tileset3d tileset, Path source) {
    if (tileset.getRoot().getContent().isEmpty()
        || tileset.getRoot().getContent().get().getUri().isBlank()) {
      throw new IllegalStateException(
          "No root content URI found in 3D Tiles tileset file: " + source);
    }

    if (tileset.getRoot().getImplicitTiling().isEmpty()
        || tileset.getRoot().getImplicitTiling().get().getSubtrees().getUri().isBlank()) {
      throw new IllegalStateException(
          "No implicit tiling subtree URI found in 3D Tiles tileset file: " + source);
    }

    return Tile3dStorePlain.readOnly(
        rootStore,
        tileset.getRoot().getContent().map(WithUri::getUri).orElse(""),
        tileset
            .getRoot()
            .getImplicitTiling()
            .map(implicitTiling -> implicitTiling.getSubtrees().getUri())
            .orElse(""));
  }

  private Tile3dStoreReadOnly loadStoreExplicit(Tileset3d tileset, Path source) {
    Map<String, Path> contentUris = new LinkedHashMap<>();

    tileset
        .getRoot()
        .accept(
            tile -> {
              tile.getContent()
                  .map(WithUri::getUri)
                  .ifPresent(
                      uri ->
                          contentUris.put(
                              Tile3d.flattenUri(uri), Path.of(source.getParent().toString(), uri)));
            });

    if (contentUris.isEmpty()) {
      throw new IllegalStateException("No content URIs found in 3D Tiles tileset file: " + source);
    }

    return Tile3dStorePlainExplicit.readOnly(rootStore, contentUris);
  }

  @Override
  public Optional<Tileset3d> getMetadata(String tilesetId) {
    return Optional.ofNullable(metadata.get(tilesetId));
  }

  @Override
  public TileResult getSubtree(Tile3dQuery tileQuery) {
    Optional<TileResult> error = validate(tileQuery);

    if (error.isPresent()) {
      return error.get();
    }

    try {
      return stores.get(tileQuery.getTileset()).getSubtree(tileQuery);
    } catch (Throwable e) {
      return TileResult.error(String.format("Could not access subtree: %s", e.getMessage()));
    }
  }

  @Override
  public TileResult getTile(Tile3dQuery tileQuery) {
    Optional<TileResult> error = validate(tileQuery);

    if (error.isPresent()) {
      return error.get();
    }

    try {
      return stores.get(tileQuery.getTileset()).get(tileQuery);
    } catch (Throwable e) {
      return TileResult.error(String.format("Could not access subtree: %s", e.getMessage()));
    }
  }

  @Override
  public boolean tilesMayBeUnavailable() {
    return true;
  }

  private void loadMetadata(Map<String, Path> tilesetSources) {
    tilesetSources.forEach(
        (key, path) -> {
          Tuple<String, String> tilesetKey = toTuple(key);

          metadata.put(tilesetKey.first(), loadMetadata(tilesetKey.second(), path));
          // TODO tmsRanges.put(tilesetKey.first(),
          // metadata.get(tilesetKey.first()).getTmsRanges());
        });
  }

  private Tileset3d loadMetadata(String tms, Path path) {
    try {
      InputStream inputStream = rootStore.content(path).orElseThrow();
      Tileset3d tileset = objectMapper.readValue(inputStream, Tileset3d.class);
      return tileset;
    } catch (Throwable e) {
      throw new RuntimeException("Could not derive metadata from Mbtiles tile provider.", e);
    }

    /*try {
      MbtilesMetadata metadata = new MbtilesTileset(path, false).getMetadata();
      TileMatrixSet tileMatrixSet =
          tileMatrixSetRepository
              .get(tms)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          String.format("Unknown tile matrix set: '%s'.", tms)));
      int minzoom = metadata.getMinzoom().orElse(tileMatrixSet.getMinLevel());
      int maxzoom = metadata.getMaxzoom().orElse(tileMatrixSet.getMaxLevel());
      Optional<Integer> defzoom =
          metadata.getCenter().size() == 3
              ? Optional.of(Math.round(metadata.getCenter().get(2).floatValue()))
              : Optional.empty();
      Optional<LonLat> center =
          metadata.getCenter().size() >= 2
              ? Optional.of(
                  LonLat.of(
                      metadata.getCenter().get(0).doubleValue(),
                      metadata.getCenter().get(1).doubleValue()))
              : Optional.empty();
      Map<String, MinMax> zoomLevels =
          ImmutableMap.of(
              tms,
              new ImmutableMinMax.Builder().min(minzoom).max(maxzoom).getDefault(defzoom).build());
      List<Double> bbox = metadata.getBounds();
      Optional<BoundingBox> bounds =
          bbox.size() == 4
              ? Optional.of(
                  BoundingBox.of(bbox.get(0), bbox.get(1), bbox.get(2), bbox.get(3), OgcCrs.CRS84))
              : Optional.empty();
      TilesFormat format = metadata.getFormat();
      List<FeatureSchema> vectorSchemas =
          metadata.getVectorLayers().stream()
              .map(VectorLayer::toFeatureSchema)
              .collect(Collectors.toList());

      return ImmutableTilesetMetadata.builder()
          .addEncodings(format)
          .levels(zoomLevels)
          .center(center)
          .bounds(bounds)
          .vectorSchemas(vectorSchemas)
          .build();
    } catch (Exception e) {
      throw new RuntimeException("Could not derive metadata from Mbtiles tile provider.", e);
    }*/
  }

  private String toTilesetKey(String tileset, String tms) {
    return String.join("/", tileset, tms);
  }

  private Tuple<String, String> toTuple(String tilesetKey) {
    String[] split = tilesetKey.split("/");
    return Tuple.of(split[0], split[1]);
  }
}
