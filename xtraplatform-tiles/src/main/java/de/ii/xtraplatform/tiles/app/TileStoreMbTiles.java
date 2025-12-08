/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.tiles.domain.Cache.Storage;
import de.ii.xtraplatform.tiles.domain.ImmutableMbtilesMetadata;
import de.ii.xtraplatform.tiles.domain.ImmutableVectorLayer;
import de.ii.xtraplatform.tiles.domain.MbtilesMetadata;
import de.ii.xtraplatform.tiles.domain.MbtilesTileset;
import de.ii.xtraplatform.tiles.domain.TileGenerationSchema;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetBase;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetLimits;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetRepository;
import de.ii.xtraplatform.tiles.domain.TileQuery;
import de.ii.xtraplatform.tiles.domain.TileResult;
import de.ii.xtraplatform.tiles.domain.TileStore;
import de.ii.xtraplatform.tiles.domain.TileStoreReadOnly;
import de.ii.xtraplatform.tiles.domain.TilesFormat;
import de.ii.xtraplatform.tiles.domain.VectorLayer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteException;

public class TileStoreMbTiles implements TileStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileStoreMbTiles.class);
  public static final String MBTILES_SUFFIX = ".mbtiles";

  static TileStoreReadOnly readOnly(Map<String, Path> tileSetSources) {
    Map<String, MbtilesTileset> tileSets =
        tileSetSources.entrySet().stream()
            .map(
                entry ->
                    new SimpleImmutableEntry<>(
                        entry.getKey(), new MbtilesTileset(entry.getValue(), false)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return new TileStoreMbTiles("", null, tileSets, Map.of(), Optional.empty(), Optional.empty());
  }

  static TileStore readWrite(
      ResourceStore rootStore,
      String providerId,
      Map<String, Map<String, TileGenerationSchema>> tileSchemas,
      Map<String, Set<String>> tileMatrixSets,
      Optional<TileMatrixSetRepository> tileMatrixSetRepository,
      Optional<TileStorePartitions> partitions,
      boolean seeded) {
    Map<String, MbtilesTileset> tileSets = new ConcurrentHashMap<>();
    try {
      for (String tileset : tileSchemas.keySet()) {
        for (String tms : tileMatrixSets.get(tileset)) {
          tileSets.put(
              key(tileset, tms),
              createTileSet(
                  rootStore,
                  providerId,
                  tileset,
                  tms,
                  getVectorLayers(tileSchemas, tileset),
                  partitions,
                  tileSchemas.get(tileset).isEmpty(),
                  seeded));
        }
      }
    } catch (IOException e) {
      LogContext.errorAsWarn(LOGGER, e, "Error when loading tile caches");
    } catch (RuntimeException e) {
      if (e.getCause() instanceof IOException) {
        LogContext.errorAsWarn(LOGGER, e.getCause(), "Error when loading tile caches");
      }
      throw e;
    }
    return new TileStoreMbTiles(
        providerId, rootStore, tileSets, tileSchemas, partitions, tileMatrixSetRepository);
  }

  private final String providerId;
  private final ResourceStore rootStore;
  private final Map<String, Map<String, TileGenerationSchema>> tileSchemas;
  private final Map<String, MbtilesTileset> tileSets;
  private final Optional<TileStorePartitions> partitions;
  // the tile matrix set is only necessary for writable MBTiles files,
  // i.e., caches that are used for seeding
  private final Optional<TileMatrixSetRepository> tileMatrixSetRepository;

  private TileStoreMbTiles(
      String providerId,
      ResourceStore rootStore,
      Map<String, MbtilesTileset> tileSets,
      Map<String, Map<String, TileGenerationSchema>> tileSchemas,
      Optional<TileStorePartitions> partitions,
      Optional<TileMatrixSetRepository> tileMatrixSetRepository) {
    this.providerId = providerId;
    this.rootStore = rootStore;
    this.tileSchemas = tileSchemas;
    this.tileSets = tileSets;
    this.partitions = partitions;
    this.tileMatrixSetRepository = tileMatrixSetRepository;
  }

  @Override
  public boolean has(TileQuery tile) {
    try {
      return tileSets.containsKey(key(tile)) && tileSets.get(key(tile)).tileExists(tile);
    } catch (SQLException | IOException e) {
      // this test is only used during seeding to check, if a tile already exists;
      // if this cannot be determined due to a locked db, we assume that the tile
      // does not yet exist; the worst case is that the tile is unnecessarily reseeded
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "Could not determine, if tile {}/{}/{}/{} in tileset '{}' exists. Proceeding with the assumption that the tiles does not yet exist. Reason: {}.",
            tile.getTileMatrixSet().getId(),
            tile.getLevel(),
            tile.getRow(),
            tile.getCol(),
            tile.getTileset(),
            e.getMessage());
      }
    }
    return false;
  }

  @Override
  public TileResult get(TileQuery tile) throws IOException {
    if (!tileSets.containsKey(key(tile))) {
      return TileResult.notFound();
    }

    try {
      Optional<InputStream> content = tileSets.get(key(tile)).getTile(tile);

      if (content.isEmpty()) {
        return TileResult.notFound();
      }

      return TileResult.found(content.get().readAllBytes());
    } catch (SQLException e) {
      if (e instanceof SQLiteException && ((SQLiteException) e).getResultCode().code == 776) {
        return TileResult.notFound();
      }
      return TileResult.error(e.getMessage());
    }
  }

  @Override
  public Optional<Boolean> isEmpty(TileQuery tile) throws IOException {
    try {
      if (tileSets.containsKey(key(tile))) {
        return tileSets.get(key(tile)).tileIsEmpty(tile);
      }
    } catch (SQLException e) {
      // This information is only relevant for the optional "OATiles-hint" header,
      // we only log this on debug level
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Could not determine, if tile {}/{}/{}/{} in tileset '{}' is empty. Reason: {}",
            tile.getTileMatrixSet().getId(),
            tile.getLevel(),
            tile.getRow(),
            tile.getCol(),
            tile.getTileset(),
            e.getMessage());
      }
    }
    return Optional.empty();
  }

  @Override
  public boolean isEmpty() throws IOException {
    return tileSets.isEmpty()
        || tileSets.values().stream()
            .allMatch(
                mbtilesTileset -> {
                  try {
                    return !mbtilesTileset.hasAnyTiles();
                  } catch (SQLException | IOException e) {
                    if (LOGGER.isWarnEnabled()) {
                      LOGGER.warn(
                          "Could not determine existence of tiles in an MBTiles file. Proceeding with the assumption that tiles exist.");
                    }
                    return false;
                  }
                });
  }

  @Override
  public void put(TileQuery tile, InputStream content) throws IOException {
    synchronized (tileSets) {
      if (!tileSets.containsKey(key(tile))) {
        tileSets.put(
            key(tile),
            createTileSet(
                rootStore,
                providerId,
                tile.getTileset(),
                tile.getTileMatrixSet().getId(),
                getVectorLayers(tileSchemas, tile.getTileset()),
                partitions,
                false,
                false));
      }
    }
    MbtilesTileset tileset = tileSets.get(key(tile));
    boolean written = false;
    int count = 0;
    String reason = null;
    while (!written && count++ < 3) {
      try {
        tileset.writeTile(tile, content.readAllBytes());
        written = true;
      } catch (SQLException e) {
        reason = e.getMessage();
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              "Failed to write tile {}/{}/{}/{} for tileset '{}'. Reason: {}. Trying again...",
              tile.getTileMatrixSet().getId(),
              tile.getLevel(),
              tile.getRow(),
              tile.getCol(),
              tile.getTileset(),
              reason);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignore) {
          // ignore
        }
      }
    }

    if (!written) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "Failed to write tile {}/{}/{}/{} for tileset '{}'. Reason: {}.",
            tile.getTileMatrixSet().getId(),
            tile.getLevel(),
            tile.getRow(),
            tile.getCol(),
            tile.getTileset(),
            reason);
      }
    }
  }

  @Override
  public void delete(TileQuery tile) throws IOException {
    try {
      if (tileSets.containsKey(key(tile))) {
        tileSets.get(key(tile)).deleteTile(tile);
      }
    } catch (SQLException e) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "Failed to delete tile {}/{}/{}/{} for tileset '{}'. Reason: {}",
            tile.getTileMatrixSet().getId(),
            tile.getLevel(),
            tile.getRow(),
            tile.getCol(),
            tile.getTileset(),
            e.getMessage());
        if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
          LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
        }
      }
    }
  }

  @Override
  public void delete(
      String tileset, TileMatrixSetBase tileMatrixSet, TileMatrixSetLimits limits, boolean inverse)
      throws IOException {
    try {
      if (tileSets.containsKey(key(tileset, tileMatrixSet))) {
        tileSets.get(key(tileset, tileMatrixSet)).deleteTiles(tileMatrixSet, limits);
      }
    } catch (SQLException e) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "Failed to delete tiles {}/{}/{}-{}/{}-{} for tileset '{}'. Reason: {}",
            tileMatrixSet,
            limits.getTileMatrix(),
            limits.getMinTileRow(),
            limits.getMaxTileRow(),
            limits.getMinTileCol(),
            limits.getMaxTileCol(),
            tileset,
            e.getMessage());
        if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
          LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
        }
      }
    }
  }

  @Override
  public void walk(Walker walker) {
    tileSets.forEach(
        (key, mbtiles) -> {
          String[] fromKey = fromKey(key);
          try {
            mbtiles.walk(
                (level, row, col) -> {
                  walker.walk(
                      fromKey[0], fromKey[1], level, getXyzRow(fromKey[1], level, row), col);
                });
          } catch (SQLException | IOException e) {
            // ignore
          }
        });
  }

  @Override
  public boolean has(String tileset, String tms, int level, int row, int col) throws IOException {
    try {
      return tileSets.containsKey(key(tileset, tms))
          && tileSets
              .get(key(tileset, tms))
              .tileExists(level, row, getTmsRow(tms, level, row), col);
    } catch (SQLException | IOException e) {
      // ignore
    }
    return false;
  }

  @Override
  public void delete(String tileset, String tms, int level, int row, int col) throws IOException {
    try {
      if (tileSets.containsKey(key(tileset, tms)))
        tileSets
            .get(key(tileset, tms))
            .deleteTile(level, row, getTmsRow(tms, level, row), col, false);
    } catch (SQLException | IOException e) {
      // ignore
    }
  }

  @Override
  public Storage getStorageType() {
    return partitions.isEmpty() ? Storage.PER_TILESET : Storage.PER_JOB;
  }

  @Override
  public Optional<String> getStorageInfo(
      String tileset, String tileMatrixSet, TileMatrixSetLimits limits) {
    if (tileSets.containsKey(key(tileset, tileMatrixSet))) {
      return Optional.of(
          tileSets
              .get(key(tileset, tileMatrixSet))
              .getStorageInfo(
                  Math.max(0, Integer.parseInt(limits.getTileMatrix())),
                  limits.getMinTileRow(),
                  limits.getMinTileCol()));
    }

    return Optional.empty();
  }

  @Override
  public void tidyup() {
    tileSets.forEach(
        (key, mbtiles) -> {
          String[] fromKey = fromKey(key);
          if (tileSchemas.containsKey(fromKey[0]) && !tileSchemas.get(fromKey[0]).isEmpty()) {
            try {
              mbtiles.cleanup();
            } catch (SQLException | IOException e) {
              // ignore
            }
          }
        });
  }

  private int getTmsRow(String tmsId, int level, int row) {
    return tileMatrixSetRepository
        .flatMap(r -> r.get(tmsId))
        .map(tms -> tms.getTmsRow(level, row))
        .orElse((int) Math.pow(2, level) - row - 1);
  }

  private int getXyzRow(String tmsId, int level, int tmsRow) {
    return tileMatrixSetRepository
        .flatMap(r -> r.get(tmsId))
        .map(tms -> tms.getXyzRow(level, tmsRow))
        .orElse((int) Math.pow(2, level) - tmsRow - 1);
  }

  private static List<VectorLayer> getVectorLayers(
      Map<String, Map<String, TileGenerationSchema>> tileSchemas, String tileset) {
    return tileSchemas.get(tileset).entrySet().stream()
        .map(entry -> getVectorLayer(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  // TODO: fields, minzoom, maxzoom
  private static VectorLayer getVectorLayer(String layer, TileGenerationSchema generationSchema) {
    return ImmutableVectorLayer.builder()
        .id(layer)
        .fields(
            generationSchema.getProperties().entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        Entry::getKey,
                        entry -> VectorLayer.getTypeAsString(entry.getValue().getType()))))
        .geometryType(VectorLayer.getGeometryTypeAsString(generationSchema.getGeometryType()))
        .build();
  }

  // TODO: minzoom, maxzoom, bounds, center
  private static MbtilesTileset createTileSet(
      ResourceStore rootStore,
      String name,
      String tileset,
      String tileMatrixSet,
      List<VectorLayer> vectorLayers,
      Optional<TileStorePartitions> partitions,
      boolean isRaster,
      boolean seeded)
      throws IOException {
    Path relPath =
        Path.of(tileset).resolve(tileMatrixSet + (partitions.isEmpty() ? MBTILES_SUFFIX : ""));
    Optional<Path> filePath;

    try {
      filePath = rootStore.asLocalPath(relPath, true);
    } catch (Throwable e) {
      throw new IllegalStateException("Could not create MBTiles file.", e);
    }

    if (filePath.isEmpty()) {
      throw new IllegalStateException(
          "Could not create MBTiles file. Make sure you have a writable localizable source defined in cfg.yml.");
    }

    MbtilesMetadata md =
        ImmutableMbtilesMetadata.builder()
            .name(name)
            .format(isRaster ? TilesFormat.PNG : TilesFormat.MVT)
            .vectorLayers(vectorLayers)
            .build();

    if (partitions.isPresent()) {
      return new MbtilesTileset(filePath.get(), md, partitions, isRaster, seeded);
    }

    if (rootStore.has(relPath)) {
      return new MbtilesTileset(filePath.get(), isRaster);
    }

    try {
      return new MbtilesTileset(filePath.get(), md, Optional.empty(), isRaster, seeded);
    } catch (FileAlreadyExistsException e) {
      throw new IllegalStateException(
          "A MBTiles file already exists. It must have been created by a parallel thread, which should not occur. MBTiles file creation must be synchronized.");
    }
  }

  private static String key(TileQuery tile) {
    return key(tile.getTileset(), tile.getTileMatrixSet());
  }

  private static String key(String tileset, TileMatrixSetBase tileMatrixSet) {
    return key(tileset, tileMatrixSet.getId());
  }

  private static String key(String tileset, String tileMatrixSet) {
    return String.join("/", tileset, tileMatrixSet);
  }

  private static String[] fromKey(String key) {
    return key.split("/");
  }
}
