/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedInject;
import de.ii.xtraplatform.base.domain.Jackson;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.blobs.domain.Blob;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.entities.domain.Entity;
import de.ii.xtraplatform.entities.domain.Entity.SubType;
import de.ii.xtraplatform.features.domain.ProviderData;
import de.ii.xtraplatform.tiles.domain.TileResult;
import de.ii.xtraplatform.tiles3d.domain.Tile3dAccess;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProvider;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dProviderFilesData;
import de.ii.xtraplatform.tiles3d.domain.Tile3dQuery;
import de.ii.xtraplatform.tiles3d.domain.Tile3dStoreReadOnly;
import de.ii.xtraplatform.tiles3d.domain.Tileset3dFiles;
import de.ii.xtraplatform.tiles3d.domain.spec.Tileset3d;
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
  private final VolatileRegistry volatileRegistry;
  private final Map<String, Tileset3d> metadata;
  private final Map<String, Tile3dStoreReadOnly> stores;
  private final ObjectMapper objectMapper;

  @AssistedInject
  public Tile3dProviderFiles(
      ResourceStore blobStore,
      VolatileRegistry volatileRegistry,
      Jackson jackson,
      @Assisted Tile3dProviderFilesData data) {
    super(volatileRegistry, data, "access");

    this.rootStore = blobStore.with(Tile3dProvider.STORE_DIR_NAME);
    this.volatileRegistry = volatileRegistry;
    this.objectMapper = jackson.getDefaultObjectMapper();
    this.metadata = new LinkedHashMap<>();
    this.stores = new LinkedHashMap<>();
  }

  @Override
  protected boolean onStartup() throws InterruptedException {
    onVolatileStart();

    addSubcomponent(rootStore, "access");

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

        tileset.validate(source);

        metadata.put(entry.getKey(), tileset);

        Tile3dStoreReadOnly store =
            Tile3dStorePlain.readOnly(rootStore.with(source.getParent().toString()));

        stores.put(entry.getKey(), store);

      } catch (IOException e) {
        throw new IllegalStateException("Could not load 3D Tiles tileset file: " + source, e);
      }
    }

    return true;
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
}
