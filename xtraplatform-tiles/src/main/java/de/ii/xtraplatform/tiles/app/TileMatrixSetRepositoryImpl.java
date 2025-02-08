/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import de.ii.xtraplatform.base.domain.AppLifeCycle;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.base.domain.resiliency.AbstractVolatile;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.tiles.domain.TileMatrixSet;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetData;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetRepository;
import de.ii.xtraplatform.values.domain.ValueStore;
import de.ii.xtraplatform.values.domain.Values;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Access to the cache for tile files. */
@Singleton
@AutoBind
public class TileMatrixSetRepositoryImpl extends AbstractVolatile
    implements TileMatrixSetRepository, AppLifeCycle {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileMatrixSetRepositoryImpl.class);
  private final Values<TileMatrixSetData> customTileMatrixSetsStore;
  private final Map<String, TileMatrixSet> tileMatrixSets;
  private final VolatileRegistry volatileRegistry;

  /** set data directory */
  @Inject
  public TileMatrixSetRepositoryImpl(ValueStore valueStore, VolatileRegistry volatileRegistry) {
    super(volatileRegistry, true, "app/tilematrixsets");
    this.customTileMatrixSetsStore = valueStore.forType(TileMatrixSetData.class);
    this.tileMatrixSets = new LinkedHashMap<>();
    this.volatileRegistry = volatileRegistry;
  }

  @Override
  public CompletionStage<Void> onStart(boolean isStartupAsync) {
    onVolatileStart();
    return volatileRegistry.onAvailable(customTileMatrixSetsStore).thenRun(this::initCache);
  }

  @Override
  public Optional<TileMatrixSet> get(String tileMatrixSetId) {
    return Optional.ofNullable(tileMatrixSets.get(tileMatrixSetId));
  }

  @Override
  public Map<String, TileMatrixSet> getAll() {
    return new ImmutableMap.Builder<String, TileMatrixSet>().putAll(tileMatrixSets).build();
  }

  private void initCache() {
    PREDEFINED_TILE_MATRIX_SETS.forEach(
        tileMatrixSetId ->
            fromWellKnownId(tileMatrixSetId)
                .ifPresent(tms -> tileMatrixSets.put(tileMatrixSetId, tms)));

    customTileMatrixSetsStore
        .identifiers()
        .forEach(
            identifier -> {
              TileMatrixSetData tileMatrixSetData = customTileMatrixSetsStore.get(identifier);

              tileMatrixSets.put(
                  tileMatrixSetData.getId(), new TileMatrixSetImpl(tileMatrixSetData));
            });

    setState(State.AVAILABLE);
  }

  static Optional<TileMatrixSet> fromWellKnownId(String tileMatrixSetId) {
    InputStream inputStream;
    try {
      inputStream =
          Resources.asByteSource(
                  Resources.getResource(
                      TileMatrixSetImpl.class, "/tilematrixsets/" + tileMatrixSetId + ".json"))
              .openStream();
    } catch (IllegalArgumentException e) {
      LOGGER.debug("Tile matrix set '{}' not found: {}", tileMatrixSetId, e.getMessage());
      return Optional.empty();
    } catch (IOException e) {
      LOGGER.error("Could not load tile matrix set '{}': {}", tileMatrixSetId, e.getMessage());
      if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
        LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
      }
      return Optional.empty();
    }

    return fromInputStream(inputStream, tileMatrixSetId);
  }

  static Optional<TileMatrixSet> fromInputStream(
      InputStream tileMatrixSetInputStream, String tileMatrixSetId) {
    // prepare Jackson mapper for deserialization
    final ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new GuavaModule());
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    mapper.enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);

    TileMatrixSetData data;
    try {
      data = mapper.readValue(tileMatrixSetInputStream, TileMatrixSetData.class);
    } catch (IOException e) {
      LOGGER.error(
          "Could not deserialize tile matrix set '{}': {}", tileMatrixSetId, e.getMessage());
      if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
        LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
      }
      return Optional.empty();
    }

    return Optional.of(new TileMatrixSetImpl(data));
  }
}
