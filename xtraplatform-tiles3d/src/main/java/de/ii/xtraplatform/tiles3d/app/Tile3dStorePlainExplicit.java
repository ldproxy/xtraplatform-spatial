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
import de.ii.xtraplatform.blobs.domain.Blob;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.tiles.domain.TileResult;
import de.ii.xtraplatform.tiles3d.domain.Tile3dQuery;
import de.ii.xtraplatform.tiles3d.domain.Tile3dStoreReadOnly;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Tile3dStorePlainExplicit implements Tile3dStoreReadOnly {

  static Tile3dStoreReadOnly readOnly(
      ResourceStore blobStore,
      Map<String, Path> contentPaths,
      Map<String, byte[]> externalTilesets) {
    return new Tile3dStorePlainExplicit(blobStore, contentPaths, externalTilesets);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dStorePlainExplicit.class);
  private static final String UNKNOWN_PATH = "__unknown__";

  private final ResourceStore blobStore;
  private final Map<String, Path> contentPaths;
  private final Map<String, byte[]> externalTilesets;

  private Tile3dStorePlainExplicit(
      ResourceStore blobStore,
      Map<String, Path> contentPaths,
      Map<String, byte[]> externalTilesets) {
    this.blobStore = blobStore;
    this.contentPaths = contentPaths;
    this.externalTilesets = externalTilesets;
  }

  @Override
  public boolean has(Tile3dQuery tile) throws IOException {
    return externalTilesets.containsKey(tile.getFileName().orElse(UNKNOWN_PATH))
        || blobStore.has(path(contentPaths, tile));
  }

  @Override
  public Optional<Blob> get(Tile3dQuery tile) throws IOException {
    /*if (externalTilesets.containsKey(tile.getFileName().orElse(UNKNOWN_PATH))) {
      return TileResult.found(externalTilesets.get(tile.getFileName().orElse(UNKNOWN_PATH)));
    }*/

    Optional<Blob> content =
        blobStore.get(path(contentPaths, tile)).map(Tile3dStorePlainExplicit::applyContentType);

    /*if (content.isEmpty()) {
      return TileResult.notFound();
    }

    try (InputStream result = content.get()) {
      return TileResult.found(result.readAllBytes());
    }*/
    return content;
  }

  @Override
  public boolean hasSubtree(Tile3dQuery tile) throws IOException {
    return false;
  }

  @Override
  public TileResult getSubtree(Tile3dQuery tile) throws IOException {
    return TileResult.notFound();
  }

  @Override
  public Optional<Boolean> isEmpty(Tile3dQuery tile) throws IOException {
    if (externalTilesets.containsKey(tile.getFileName().orElse(UNKNOWN_PATH))) {
      byte[] data = externalTilesets.get(tile.getFileName().orElse(UNKNOWN_PATH));
      return Optional.of(data.length == 0);
    }

    long size = blobStore.size(path(contentPaths, tile));

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
    return false;
  }

  private static Path path(Map<String, Path> contentPaths, Tile3dQuery tile) {
    return Path.of(/*tile.getTileset(),*/ tile.getFileName().orElse(UNKNOWN_PATH));
    /*return contentPaths.getOrDefault(
    tile.getFileName().orElse(UNKNOWN_PATH), Path.of(UNKNOWN_PATH));*/
  }

  private static Blob applyContentType(Blob blob) {
    String fileName = blob.path().getFileName().toString();
    String contentType = contentType(fileName);

    return blob.withPrecomputedContentType(contentType);
  }

  private static String contentType(String fileName) {
    String ext = Files.getFileExtension(fileName).toLowerCase();

    switch (ext) {
      case "glb":
        return "model/gltf-binary";
      case "gltf":
        return "model/gltf+json";
      case "png":
        return "image/png";
      case "jpg":
      case "jpeg":
        return "image/jpeg";
      case "json":
        return "application/json";
      case "b3dm":
      case "pnts":
      case "i3dm":
      case "cmpt":
      case "bin":
      case "subtree":
      default:
        return "application/octet-stream";
    }
  }
}
