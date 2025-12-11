/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedInject;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.base.domain.MapStreams;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.blobs.domain.Blob;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.entities.domain.Entity;
import de.ii.xtraplatform.entities.domain.Entity.SubType;
import de.ii.xtraplatform.entities.domain.EntityRegistry;
import de.ii.xtraplatform.features.domain.ProviderData;
import de.ii.xtraplatform.tiles.domain.Cache;
import de.ii.xtraplatform.tiles.domain.Cache.Storage;
import de.ii.xtraplatform.tiles.domain.Cache.Type;
import de.ii.xtraplatform.tiles.domain.ImmutableCache;
import de.ii.xtraplatform.tiles.domain.MinMax;
import de.ii.xtraplatform.tiles.domain.TileCache;
import de.ii.xtraplatform.tiles.domain.TileGenerationParameters;
import de.ii.xtraplatform.tiles.domain.TileMatrixSet;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetBase;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetData;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import de.ii.xtraplatform.tiles.domain.TileResult;
import de.ii.xtraplatform.tiles.domain.TileSeeding;
import de.ii.xtraplatform.tiles.domain.TileSubMatrix;
import de.ii.xtraplatform.tiles.domain.TileWalker;
import de.ii.xtraplatform.tiles3d.domain.ImmutableSeedingOptions3d;
import de.ii.xtraplatform.tiles3d.domain.ImmutableTile3dQuery;
import de.ii.xtraplatform.tiles3d.domain.SeedingOptions3d;
import de.ii.xtraplatform.tiles3d.domain.Tile3dAccess;
import de.ii.xtraplatform.tiles3d.domain.Tile3dBuilder;
import de.ii.xtraplatform.tiles3d.domain.Tile3dCoordinates;
import de.ii.xtraplatform.tiles3d.domain.Tile3dGenerator;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderFeaturesData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dQuery;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeeding;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJob;
import de.ii.xtraplatform.tiles3d.domain.Tile3dSeedingJobSet;
import de.ii.xtraplatform.tiles3d.domain.Tile3dStore;
import de.ii.xtraplatform.tiles3d.domain.TileTree;
import de.ii.xtraplatform.tiles3d.domain.Tileset3dFeatures;
import de.ii.xtraplatform.tiles3d.domain.spec.ImmutableAssetMetadata;
import de.ii.xtraplatform.tiles3d.domain.spec.ImmutableBoundingVolume;
import de.ii.xtraplatform.tiles3d.domain.spec.ImmutableImplicitTiling;
import de.ii.xtraplatform.tiles3d.domain.spec.ImmutableTile3d;
import de.ii.xtraplatform.tiles3d.domain.spec.ImmutableTileset3d;
import de.ii.xtraplatform.tiles3d.domain.spec.ImmutableWithUri;
import de.ii.xtraplatform.tiles3d.domain.spec.Subtree;
import de.ii.xtraplatform.tiles3d.domain.spec.Tileset3d;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Entity(
    type = Tile3dProviderData.ENTITY_TYPE,
    subTypes = {
      @SubType(key = ProviderData.PROVIDER_TYPE_KEY, value = Tile3dProviderData.PROVIDER_TYPE),
      @SubType(
          key = ProviderData.PROVIDER_SUB_TYPE_KEY,
          value = Tile3dProviderFeaturesData.PROVIDER_SUBTYPE)
    },
    data = Tile3dProviderFeaturesData.class)
public class Tile3dProviderFeatures extends AbstractTile3dProvider<Tile3dProviderFeaturesData>
    implements Tile3dProvider, Tile3dAccess, Tile3dSeeding {

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dProviderFeatures.class);

  private final ResourceStore rootStore;
  private final Map<String, Tileset3d> metadata;
  private final Map<String, Tile3dStore> stores;
  private final Tile3dGenerator tileGenerator;
  private final TileWalker tileWalker;
  private final List<TileCache> generatorCaches;
  private final Map<String, Map<String, TileMatrixSetBase>> customTms;
  private final boolean asyncStartup;

  @AssistedInject
  public Tile3dProviderFeatures(
      ResourceStore blobStore,
      AppContext appContext,
      EntityRegistry entityRegistry,
      VolatileRegistry volatileRegistry,
      Cql cql,
      CrsTransformerFactory crsTransformerFactory,
      TileWalker tileWalker,
      Set<Tile3dBuilder> tileBuilders,
      @Assisted Tile3dProviderFeaturesData data) {
    super(volatileRegistry, data, "access", "seeding", "generation");

    this.rootStore =
        blobStore.with(
            Tile3dProvider.STORE_DIR_NAME, Tile3dProvider.clean(data.getId()), "cache_dyn");
    this.metadata = new LinkedHashMap<>();
    this.stores = new LinkedHashMap<>();
    this.asyncStartup = appContext.getConfiguration().getModules().isStartupAsync();
    this.tileGenerator =
        new Tile3dGeneratorFeatures(
            data,
            cql,
            crsTransformerFactory,
            entityRegistry,
            volatileRegistry,
            tileBuilders,
            appContext,
            asyncStartup);
    this.tileWalker = tileWalker;
    this.generatorCaches = new ArrayList<>();
    this.customTms = new LinkedHashMap<>();
  }

  @Override
  protected boolean onStartup() throws InterruptedException {
    onVolatileStart();

    addSubcomponent(rootStore, "access", "seeding");
    addSubcomponent(tileGenerator, true, "generation", "seeding");

    tileGenerator.init();

    if (!asyncStartup) {
      init();
    }

    return super.onStartup();
  }

  @Override
  protected Tuple<State, String> volatileInit() {
    if (asyncStartup) {
      init();
    }
    return super.volatileInit();
  }

  private void init() {
    for (Entry<String, Tileset3dFeatures> entry : getData().getTilesets().entrySet()) {
      Tileset3dFeatures tilesetCfg = entry.getValue().mergeDefaults(getData().getTilesetDefaults());

      metadata.put(tilesetCfg.getId(), getTileset(tilesetCfg));

      Tile3dStore store = Tile3dStorePlain.readWrite(rootStore.with(tilesetCfg.getId()));

      stores.put(tilesetCfg.getId(), store);
    }

    Cache cache =
        new ImmutableCache.Builder()
            .type(Type.DYNAMIC)
            .storage(Storage.PER_TILE)
            .levels(getData().getTilesetDefaults().getLevels())
            .tilesetLevels(
                getData().getTilesets().entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().getLevels())))
            .build();

    Map<String, Map<String, Range<Integer>>> cacheRanges = getCacheRanges(cache);
    Map<String, Map<String, TileMatrixSetBase>> customTms =
        cacheRanges.entrySet().stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        entry.getValue().entrySet().stream()
                            .map(
                                tmsEntry -> {
                                  String tmsId = tmsEntry.getKey();
                                  TileMatrixSetData tmsData =
                                      tileGenerator.getTileMatrixSetData(
                                          entry.getKey(), tmsId, tmsEntry.getValue());
                                  return Map.entry(
                                      tmsId, (TileMatrixSetBase) TileMatrixSet.custom(tmsData));
                                })
                            .collect(MapStreams.toMap())))
            .collect(MapStreams.toMap());

    generatorCaches.add(new Tile3dCacheDynamic(tileWalker, customTms, cacheRanges));
    this.customTms.putAll(customTms);

    // registerChangeHandlers();
  }

  @Override
  protected void onStopped() {
    // unregisterChangeHandlers();

    super.onStopped();
  }

  @Override
  public Optional<Tileset3d> getMetadata(String tilesetId) {
    return Optional.ofNullable(metadata.get(tilesetId));
  }

  @Override
  public Optional<Blob> getFile(Tile3dQuery tileQuery) throws IOException {
    Optional<TileResult> error = validate(tileQuery);

    if (error.isPresent()) {
      throw new IllegalArgumentException(error.get().getError().orElse("Invalid tile query."));
    }

    return stores.get(tileQuery.getTileset()).get(tileQuery);
  }

  @Override
  public SeedingOptions3d getOptions() {
    return getData().getSeeding().orElseGet(() -> new ImmutableSeedingOptions3d.Builder().build());
  }

  @Override
  public Map<String, Map<String, Set<TileMatrixSetLimits>>> getCoverage(
      Map<String, TileGenerationParameters> tilesets) throws IOException {
    Map<String, TileGenerationParameters> validTilesets = validTilesets(tilesets);
    Map<String, TileGenerationParameters> sourcedTilesets = sourcedTilesets(validTilesets);

    Map<String, Map<String, Set<TileMatrixSetLimits>>> coverage = new LinkedHashMap<>();

    if (!sourcedTilesets.isEmpty()) {
      for (TileCache cache : generatorCaches) {
        if (cache.isSeeded()) {
          TileSeeding.mergeCoverageInto(cache.getCoverage(sourcedTilesets), coverage);
        }
      }
    }

    return coverage;
  }

  @Override
  public void setupSeeding(Tile3dSeedingJobSet jobSet) throws IOException {
    /*for (Tuple<TileCache, String> cache : getCaches(jobSet)) {
      cache.first().setupSeeding(jobSet, cache.second());
    }*/
  }

  @Override
  public void cleanupSeeding(Tile3dSeedingJobSet jobSet) throws IOException {
    LOGGER.debug("{}: cleaning up tile caches", Tile3dSeedingJobSet.LABEL);

    /*for (Tuple<TileCache, String> cache : getCaches(jobSet)) {
      cache.first().cleanupSeeding(jobSet, cache.second());
    }*/
  }

  @Override
  public void seedSubtrees(Tile3dSeedingJob job, Consumer<Integer> updateProgress)
      throws IOException {

    Tileset3dFeatures tileset =
        getData().getTilesets().get(job.getTileSet()).mergeDefaults(getData().getTilesetDefaults());
    Tile3dStore store = stores.get(job.getTileSet());
    AtomicInteger current = new AtomicInteger(0);
    Runnable updateProgress2 =
        () -> {
          int cur = current.incrementAndGet();
          updateProgress.accept(cur);
        };

    for (TileSubMatrix t : job.getSubMatrices()) {
      try {
        if (job.isReseed() || !store.hasSubtree(t.getLevel(), t.getColMin(), t.getRowMin())) {
          TileTree child = TileTree.fromSubMatrix(t);

          if (child.getLevel() > 0) {
            TileTree parent = child.parent(tileset.getSubtreeLevels());

            if (!store.hasSubtree(parent.getLevel(), parent.getCol(), parent.getRow())) {
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "Parent subtree {}/{}/{} not available, skipping subtree {}/{}/{}",
                    parent.getLevel(),
                    parent.getCol(),
                    parent.getRow(),
                    child.getLevel(),
                    child.getCol(),
                    child.getRow());
              }
              return;
            }

            byte[] subtreeBytes =
                store.getSubtree(parent.getLevel(), parent.getCol(), parent.getRow());
            Subtree subtreeParent = tileGenerator.subtreeFromBinary(subtreeBytes);

            if (!tileGenerator.isSubtreeAvailable(
                subtreeParent, child, tileset.getSubtreeLevels())) {
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "Subtree {}/{}/{} not available, skipping",
                    t.getLevel(),
                    t.getColMin(),
                    t.getRowMin());
              }
              return;
            }
          }

          Subtree subtree = tileGenerator.generateSubtree(job.getTileSet(), child);
          byte[] subtreeAsBinary = tileGenerator.subtreeToBinary(subtree);
          store.putSubtree(t.getLevel(), t.getColMin(), t.getRowMin(), subtreeAsBinary);
        } else if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              "Subtree {}/{}/{} already exists, skipping",
              t.getLevel(),
              t.getColMin(),
              t.getRowMin());
        }
      } finally {
        updateProgress2.run();
      }
    }

    // updateProgress2.run();
  }

  @Override
  public void seedTiles(
      Tile3dSeedingJob job, Tile3dSeedingJobSet jobSet, Consumer<Integer> updateProgress)
      throws IOException {
    Tileset3dFeatures tileset =
        getData().getTilesets().get(job.getTileSet()).mergeDefaults(getData().getTilesetDefaults());
    Tile3dStore store = stores.get(job.getTileSet());
    TileMatrixSetBase tileMatrixSet = customTms.get(job.getTileSet()).get(job.getTileMatrixSet());

    AtomicInteger current = new AtomicInteger(0);
    Consumer<Integer> updateProgress2 =
        (delta) -> {
          int cur = current.addAndGet(delta);
          updateProgress.accept(cur);
        };

    for (TileSubMatrix subMatrix : job.getSubMatrices()) {
      TileTree parent = TileTree.parentOf(subMatrix, tileset.getSubtreeLevels());

      if (!store.hasSubtree(parent.getLevel(), parent.getCol(), parent.getRow())) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              "Subtree {}/{}/{} not available, skipping content {}",
              parent.getLevel(),
              parent.getCol(),
              parent.getRow(),
              subMatrix.asStringXY());
        }

        updateProgress2.accept((int) subMatrix.getNumberOfTiles());
        return;
      }

      byte[] subtreeBytes = store.getSubtree(parent.getLevel(), parent.getCol(), parent.getRow());
      Subtree subtree = tileGenerator.subtreeFromBinary(subtreeBytes);

      for (int row = subMatrix.getRowMin(); row <= subMatrix.getRowMax(); row++) {
        for (int col = subMatrix.getColMin(); col <= subMatrix.getColMax(); col++) {
          try {
            Tile3dCoordinates t =
                ImmutableTile3dQuery.builder()
                    .tileset(tileset.getId())
                    .level(subMatrix.getLevel())
                    .col(col)
                    .row(row)
                    .build();
            if (tileGenerator.isTileAvailable(subtree, t, tileset.getSubtreeLevels())) {
              if (job.isReseed() || !store.hasContent(subMatrix.getLevel(), col, row)) {
                byte[] tileBytes =
                    tileGenerator.generateTile(tileset, t, job, jobSet, tileMatrixSet);
                store.putContent(subMatrix.getLevel(), col, row, tileBytes);
              } else if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "Tile {}/{}/{} already exists, skipping", subMatrix.getLevel(), col, row);
              }
            } else if (LOGGER.isTraceEnabled()) {
              LOGGER.trace(
                  "Tile {}/{}/{} not available in subtree, skipping",
                  subMatrix.getLevel(),
                  col,
                  row);
            }
          } finally {
            updateProgress2.accept(1);
          }
        }
      }
    }
  }

  private Map<String, TileGenerationParameters> validTilesets(
      Map<String, TileGenerationParameters> tilesets) {
    return tilesets.entrySet().stream()
        .filter(
            entry -> {
              if (!getData().getTilesets().containsKey(entry.getKey())) {
                LOGGER.warn("Tileset with name '{}' not found", entry.getKey());
                return false;
              }
              return true;
            })
        .collect(MapStreams.toMap());
  }

  private Map<String, TileGenerationParameters> sourcedTilesets(
      Map<String, TileGenerationParameters> tilesets) {
    return tilesets.entrySet().stream()
        .filter(
            entry ->
                getData().getTilesets().containsKey(entry.getKey())
                    && !getData().getTilesets().get(entry.getKey()).isCombined())
        .collect(MapStreams.toMap());
  }

  private Tileset3d getTileset(Tileset3dFeatures cfg) {
    BoundingBox bbox = tileGenerator.getBounds(cfg.getId()).orElseThrow();

    @SuppressWarnings("ConstantConditions")
    Tileset3d tileset =
        new ImmutableTileset3d.Builder()
            .asset(
                new ImmutableAssetMetadata.Builder()
                    .version("1.1")
                    .generator(tileGenerator.getLabel())
                    .build())
            .geometricError(10_000)
            .root(
                new ImmutableTile3d.Builder()
                    .boundingVolume(
                        new ImmutableBoundingVolume.Builder()
                            .region(
                                ImmutableList.of(
                                    Math.toRadians(bbox.getXmin()),
                                    Math.toRadians(bbox.getYmin()),
                                    Math.toRadians(bbox.getXmax()),
                                    Math.toRadians(bbox.getYmax()),
                                    Objects.requireNonNull(bbox.getZmin()),
                                    Objects.requireNonNull(bbox.getZmax())))
                            .build())
                    .geometricError(cfg.getGeometricErrorRoot())
                    .refine("ADD")
                    .content(new ImmutableWithUri.Builder().uri("{level}_{x}_{y}.glb").build())
                    .implicitTiling(
                        new ImmutableImplicitTiling.Builder()
                            .subdivisionScheme("QUADTREE")
                            .availableLevels(cfg.getContentLevels().getMax() + 1)
                            .subtreeLevels(Objects.requireNonNull(cfg.getSubtreeLevels()))
                            .subtrees(
                                new ImmutableWithUri.Builder()
                                    .uri("{level}_{x}_{y}.subtree")
                                    .build())
                            .build())
                    .build())
            .extensionsUsed(
                ImmutableList.of(
                    "3DTILES_content_gltf", "3DTILES_implicit_tiling", "3DTILES_metadata"))
            .extensionsRequired(ImmutableList.of("3DTILES_content_gltf", "3DTILES_implicit_tiling"))
            .build();
    return tileset;
  }

  private Map<String, Map<String, Range<Integer>>> getCacheRanges(Cache cache) {
    return getCacheRanges(cache, (tileset, ranges) -> ranges);
  }

  private Map<String, Map<String, Range<Integer>>> getCacheRanges(
      Cache cache,
      BiFunction<String, Map<String, Range<Integer>>, Map<String, Range<Integer>>> rangeMapper) {
    return getData().getTilesets().keySet().stream()
        .map(
            tileset ->
                new SimpleImmutableEntry<>(
                    tileset,
                    rangeMapper.apply(
                        tileset,
                        capCacheRanges(
                            mergeCacheRanges(
                                cache.getTmsRanges(), cache.getTilesetTmsRanges().get(tileset)),
                            getTilesetRanges(tileset)))))
        .collect(MapStreams.toMap());
  }

  private Map<String, Range<Integer>> mergeCacheRanges(
      Map<String, Range<Integer>> defaults, Map<String, Range<Integer>> tileset) {
    if (Objects.isNull(tileset)) {
      return defaults;
    }
    Map<String, Range<Integer>> merged = new LinkedHashMap<>();

    merged.putAll(defaults);
    merged.putAll(tileset);

    return merged;
  }

  private Map<String, Range<Integer>> capCacheRanges(
      Map<String, Range<Integer>> ranges, Map<String, Range<Integer>> capRanges) {
    return ranges.entrySet().stream()
        .filter(
            entry ->
                capRanges.containsKey(entry.getKey())
                    && capRanges.get(entry.getKey()).isConnected(entry.getValue()))
        .map(
            entry ->
                new SimpleImmutableEntry<>(
                    entry.getKey(),
                    Range.closed(
                        Math.max(
                            entry.getValue().lowerEndpoint(),
                            capRanges.get(entry.getKey()).lowerEndpoint()),
                        Math.min(
                            entry.getValue().upperEndpoint(),
                            capRanges.get(entry.getKey()).upperEndpoint()))))
        .collect(MapStreams.toMap());
  }

  private Map<String, MinMax> getTilesetLevels(Tileset3dFeatures tileset) {
    return tileset.getLevels().isEmpty()
        ? getData().getTilesetDefaults().getLevels()
        : tileset.getLevels();
  }

  private Map<String, Range<Integer>> getTilesetRanges(String tileset) {
    return getTilesetLevels(getData().getTilesets().get(tileset)).entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().asRange()));
  }
}
