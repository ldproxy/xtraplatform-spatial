/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.google.common.io.Files;
import de.ii.xtraplatform.blobs.domain.Blob;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.tiles3d.domain.Tile3dQuery;
import de.ii.xtraplatform.tiles3d.domain.Tile3dStore;
import de.ii.xtraplatform.tiles3d.domain.Tile3dStoreReadOnly;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Tile3dStorePlain implements Tile3dStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(Tile3dStorePlain.class);

  static Tile3dStoreReadOnly readOnly(ResourceStore blobStore) {
    return new Tile3dStorePlain(blobStore);
  }

  static Tile3dStore readWrite(ResourceStore blobStore) {
    return new Tile3dStorePlain(blobStore);
  }

  private static final String UNKNOWN_PATH = "__unknown__";

  private final ResourceStore blobStore;

  private Tile3dStorePlain(ResourceStore blobStore) {
    this.blobStore = blobStore;
  }

  @Override
  public boolean has(Tile3dQuery tile) throws IOException {
    return blobStore.has(path(tile));
  }

  @Override
  public Optional<Blob> get(Tile3dQuery tile) throws IOException {
    return blobStore.get(path(tile)).map(Tile3dStorePlain::applyContentType);
  }

  @Override
  public Optional<Boolean> isEmpty(Tile3dQuery tile) throws IOException {
    long size = blobStore.size(path(tile));

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
  public void putSubtree(int level, int x, int y, byte[] subtree) throws IOException {
    String fileName = String.format("%d_%d_%d.%s", level, x, y, "subtree");
    blobStore.put(Path.of(fileName), new ByteArrayInputStream(subtree));
  }

  private static Path path(Tile3dQuery tile) {
    return Path.of(tile.getFileName().orElse(UNKNOWN_PATH));
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
