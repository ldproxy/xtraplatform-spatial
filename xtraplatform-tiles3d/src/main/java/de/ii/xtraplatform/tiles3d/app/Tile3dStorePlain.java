/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.google.common.io.Files;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.tiles.domain.TileResult;
import de.ii.xtraplatform.tiles3d.domain.Tile3dQuery;
import de.ii.xtraplatform.tiles3d.domain.Tile3dStoreReadOnly;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Tile3dStorePlain implements Tile3dStoreReadOnly {

  static Tile3dStoreReadOnly readOnly(
      ResourceStore blobStore,
      String prefix,
      String contentPathTemplate,
      String subtreePathTemplate) {
    return new Tile3dStorePlain(
        blobStore,
        Path.of(prefix, contentPathTemplate).toString(),
        Path.of(prefix, subtreePathTemplate).toString());
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dStorePlain.class);

  private final ResourceStore blobStore;
  private final String contentPathTemplate;
  private final String subtreePathTemplate;

  private Tile3dStorePlain(
      ResourceStore blobStore, String contentPathTemplate, String subtreePathTemplate) {
    this.blobStore = blobStore;
    this.contentPathTemplate = contentPathTemplate;
    this.subtreePathTemplate = subtreePathTemplate;
  }

  @Override
  public boolean has(Tile3dQuery tile) throws IOException {
    return blobStore.has(path(contentPathTemplate, tile));
  }

  @Override
  public TileResult get(Tile3dQuery tile) throws IOException {
    Optional<InputStream> content = blobStore.content(path(contentPathTemplate, tile));

    if (content.isEmpty()) {
      return TileResult.notFound();
    }

    try (InputStream result = content.get()) {
      return TileResult.found(result.readAllBytes());
    }
  }

  @Override
  public boolean hasSubtree(Tile3dQuery tile) throws IOException {
    return blobStore.has(path(subtreePathTemplate, tile));
  }

  @Override
  public TileResult getSubtree(Tile3dQuery tile) throws IOException {
    Optional<InputStream> content = blobStore.content(path(subtreePathTemplate, tile));

    if (content.isEmpty()) {
      return TileResult.notFound();
    }

    try (InputStream result = content.get()) {
      return TileResult.found(result.readAllBytes());
    }
  }

  @Override
  public Optional<Boolean> isEmpty(Tile3dQuery tile) throws IOException {
    long size = blobStore.size(path(contentPathTemplate, tile));

    return size < 0 ? Optional.empty() : Optional.of(size == 0);
  }

  @Override
  public boolean isEmpty() throws IOException {
    try (Stream<Path> paths = blobStore.walk(Path.of(""), 5, (p, a) -> a.isValue())) {
      return paths.findAny().isEmpty();
    } catch (IOException e) {
      // ignore
    }

    return false;
  }

  @Override
  public void walk(Walker walker) {
    try (Stream<Path> paths = blobStore.walk(Path.of(""), 5, (p, a) -> a.isValue())) {
      paths.forEach(
          path -> {
            if (path.getNameCount() == 5) {
              walker.walk(
                  path.getName(0).toString(),
                  path.getName(1).toString(),
                  Integer.parseInt(path.getName(2).toString()),
                  Integer.parseInt(path.getName(3).toString()),
                  Integer.parseInt(Files.getNameWithoutExtension(path.getName(4).toString())));
            }
          });
    } catch (IOException e) {
      LogContext.errorAsDebug(
          LOGGER, e, "Could not walk cache level tiles {}.", blobStore.getPrefix());
    }
  }

  @Override
  public boolean has(String tileset, String tms, int level, int row, int col) throws IOException {
    return blobStore.has(path(contentPathTemplate, level, row, col));
  }

  /*@Override
  public void put(Tile3dQuery tile, InputStream content) throws IOException {
    blobStore.put(path(tile), content);
  }

  @Override
  public void delete(Tile3dQuery tile) throws IOException {
    blobStore.delete(path(tile));
  }

  @Override
  public void delete(
      String tileset, TileMatrixSetBase tileMatrixSet, TileMatrixSetLimits limits, boolean inverse)
      throws IOException {
    try (Stream<Path> matchingFiles =
        blobStore.walk(
            Path.of(""),
            5,
            (path, fileAttributes) ->
                fileAttributes.isValue()
                    && TileStore.isInsideBounds(
                        path, tileset, tileMatrixSet.getId(), limits, inverse))) {

      try {
        matchingFiles.forEach(consumerMayThrow(blobStore::delete));
      } catch (RuntimeException e) {
        if (e instanceof UncheckedIOException && e.getCause() instanceof NoSuchFileException) {
          // ignore
          return;
        }
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw e;
      }
    }
  }

  @Override
  public void delete(String tileset, String tms, int level, int row, int col) throws IOException {
    blobStore.delete(path(tileset, tms, level, row, col));
  }

  @Override
  public Storage getStorageType() {
    return Storage.PER_TILE;
  }

  @Override
  public Optional<String> getStorageInfo(
      String tileset, String tileMatrixSet, TileMatrixSetLimits limits) {
    try {
      Optional<Path> path =
          blobStore.asLocalPath(
              path(
                  tileset,
                  tileMatrixSet,
                  Math.max(0, Integer.parseInt(limits.getTileMatrix())),
                  limits.getMinTileRow(),
                  limits.getMinTileCol()),
              true);

      return path.map(Path::toString)
          .map(
              p ->
                  p.replace(
                      Integer.toString(limits.getMinTileRow())
                          + "/"
                          + Integer.toString(limits.getMinTileCol()),
                      "{row}/{col}"));
    } catch (IOException e) {
      // ignore
    }

    return Optional.empty();
  }*/

  private static Path path(String template, Tile3dQuery tile) {
    return path(template, tile.getLevel(), tile.getRow(), tile.getCol());
  }

  private static Path path(String template, int level, int row, int col) {
    return Path.of(
        template
            .replace("{level}", String.valueOf(level))
            .replace("{x}", String.valueOf(col))
            .replace("{y}", String.valueOf(row)));
  }
}
