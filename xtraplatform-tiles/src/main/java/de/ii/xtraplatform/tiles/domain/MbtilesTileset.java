/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import static de.ii.xtraplatform.tiles.app.TileStoreMbTiles.MBTILES_SUFFIX;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.tiles.app.FeatureEncoderMVT;
import de.ii.xtraplatform.tiles.app.SqlHelper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MbtilesTileset {

  interface Mutex {
    boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;

    void release();

    static Mutex create() {
      Semaphore semaphore = new Semaphore(1);

      return new Mutex() {
        @Override
        public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
          return semaphore.tryAcquire(timeout, unit);
        }

        @Override
        public void release() {
          semaphore.release();
        }
      };
    }

    static Mutex createNoOp() {
      return new Mutex() {
        @Override
        public boolean tryAcquire(long timeout, TimeUnit unit) {
          return true;
        }

        @Override
        public void release() {}
      };
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(MbtilesTileset.class);
  private static final int EMPTY_TILE_ID = 1;
  private static final int IDS_CHUCK_SIZE = 10000;
  private final Path tilesetPath;
  private final Mutex mutex;
  private final MbtilesMetadata metadata;
  private final Optional<TileMatrixPartitions> partitions;
  private final boolean numericTileIds;
  private final String tileMapTable;
  private final String tileBlobsTable;

  public MbtilesTileset(Path tilesetPath, boolean isXtratiler) {
    this(tilesetPath, null, Optional.empty(), isXtratiler, true, Mutex.create());
  }

  public MbtilesTileset(
      Path tilesetPath,
      MbtilesMetadata metadata,
      Optional<TileMatrixPartitions> partitions,
      boolean isXtratiler)
      throws IOException {
    this(
        tilesetPath,
        metadata,
        partitions,
        isXtratiler,
        false,
        partitions.isPresent() ? Mutex.createNoOp() : Mutex.create());
  }

  private MbtilesTileset(
      Path tilesetPath,
      MbtilesMetadata metadata,
      Optional<TileMatrixPartitions> partitions,
      boolean isXtratiler,
      boolean mustExist,
      Mutex mutex) {
    if (mustExist && !Files.exists(tilesetPath)) {
      throw new IllegalStateException(
          String.format("Mbtiles file does not exist: %s", tilesetPath));
    }

    this.tilesetPath = tilesetPath;
    this.partitions = partitions;
    this.mutex = mutex;
    this.numericTileIds = !isXtratiler;
    this.tileMapTable = isXtratiler ? "map" : "tile_map";
    this.tileBlobsTable = isXtratiler ? "images" : "tile_blobs";

    if (Objects.isNull(metadata)) {
      try {
        this.metadata = getMetadata();
      } catch (SQLException | IOException e) {
        throw new IllegalStateException(
            String.format("Could not read metadata from Mbtiles file: %s", tilesetPath), e);
      }
    } else {
      this.metadata = metadata;
    }
  }

  private Path getTilesetPath(TileCoordinates tile) {
    if (partitions.isPresent()) {
      return getTilesetPath(tile.getLevel(), tile.getRow(), tile.getCol());
    }

    return tilesetPath;
  }

  private Path getTilesetPath(int level, int row, int col) {
    if (partitions.isPresent()) {
      String partition = partitions.get().getPartitionName(level, row, col);
      return tilesetPath.resolve(partition + MBTILES_SUFFIX);
    }

    return tilesetPath;
  }

  private List<Path> getTilesetPaths() {
    if (partitions.isEmpty()) {
      return List.of(tilesetPath);
    }

    if (!Files.isDirectory(tilesetPath)) {
      return List.of();
    }

    try (Stream<Path> files =
        Files.find(
            tilesetPath,
            1,
            ((path1, basicFileAttributes) ->
                basicFileAttributes.isRegularFile()
                    && path1.getFileName().toString().endsWith(MBTILES_SUFFIX)))) {
      return files.collect(Collectors.toList());
    } catch (IOException e) {
      LogContext.errorAsDebug(
          LOGGER, e, "Could not list MBTiles files in directory: {}", tilesetPath);
      return List.of();
    }
  }

  private void initMbtilesDb(MbtilesMetadata metadata, Connection connection)
      throws SQLException, IOException {
    try {
      // change settings to reduce overheads at the cost of protection against corrupt databases
      // in case of an error
      SqlHelper.execute(connection, "PRAGMA journal_mode=WAL");
      SqlHelper.execute(connection, "PRAGMA temp_store=MEMORY");
      SqlHelper.execute(connection, "PRAGMA locking_mode=EXCLUSIVE");
      SqlHelper.execute(connection, "PRAGMA synchronous=NORMAL");
      // create tables and views
      SqlHelper.execute(connection, "BEGIN TRANSACTION IMMEDIATE");
      SqlHelper.execute(connection, "CREATE TABLE metadata (name text, value text)");
      SqlHelper.execute(
          connection,
          "CREATE TABLE tile_map (zoom_level integer, tile_column integer, tile_row integer, tile_id integer)");
      SqlHelper.execute(
          connection,
          "CREATE UNIQUE INDEX tile_index on tile_map (zoom_level, tile_column, tile_row)");
      SqlHelper.execute(
          connection, "CREATE TABLE tile_blobs (tile_id integer primary key, tile_data blob)");
      SqlHelper.execute(
          connection,
          "CREATE VIEW tiles AS SELECT zoom_level, tile_column, tile_row, tile_data FROM tile_map INNER JOIN tile_blobs ON tile_map.tile_id = tile_blobs.tile_id");

      // populate metadata
      SqlHelper.addMetadata(connection, "name", metadata.getName());
      SqlHelper.addMetadata(connection, "format", metadata.getFormat().asMbtilesString());
      if (metadata.getBounds().size() == 4)
        SqlHelper.addMetadata(
            connection,
            "bounds",
            metadata.getBounds().stream()
                .map(v -> String.format(Locale.US, "%f", v))
                .collect(Collectors.joining(",")));
      if (metadata.getCenter().size() == 3)
        SqlHelper.addMetadata(
            connection,
            "center",
            metadata.getCenter().stream()
                .map(v -> String.format(Locale.US, "%s", v))
                .collect(Collectors.joining(",")));
      metadata.getMinzoom().ifPresent(v -> SqlHelper.addMetadata(connection, "minzoom", v));
      metadata.getMaxzoom().ifPresent(v -> SqlHelper.addMetadata(connection, "maxzoom", v));
      metadata.getAttribution().ifPresent(v -> SqlHelper.addMetadata(connection, "attribution", v));
      metadata.getDescription().ifPresent(v -> SqlHelper.addMetadata(connection, "description", v));
      metadata.getType().ifPresent(v -> SqlHelper.addMetadata(connection, "type", v));
      metadata.getVersion().ifPresent(v -> SqlHelper.addMetadata(connection, "version", v));
      if (metadata.getFormat() == TilesFormat.MVT) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        try {
          SqlHelper.addMetadata(
              connection,
              "json",
              mapper.writeValueAsString(
                  ImmutableMap.of("vector_layers", metadata.getVectorLayers())));
        } catch (JsonProcessingException e) {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error(
                String.format("Could not write 'json' metadata entry. Reason: %s", e.getMessage()));
          }
          if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
            LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
          }
          SqlHelper.addMetadata(
              connection,
              "json",
              mapper.writeValueAsString(ImmutableMap.of("vector_layers", ImmutableList.of())));
        }
      }

      // create empty MVT tile with rowid=1
      if (metadata.getFormat().equals(TilesFormat.MVT)) {
        try (PreparedStatement statement =
            connection.prepareStatement("INSERT INTO tile_blobs (tile_id,tile_data) VALUES(?,?)")) {
          statement.setInt(1, EMPTY_TILE_ID);
          ByteArrayOutputStream mvt = new ByteArrayOutputStream(0);
          GZIPOutputStream gzipStream = new GZIPOutputStream(mvt);
          gzipStream.close();
          statement.setBytes(2, mvt.toByteArray());
          statement.executeUpdate();
        }
      }

      SqlHelper.execute(connection, "COMMIT");
    } catch (SQLException | IOException e) {
      try {
        SqlHelper.execute(connection, "ROLLBACK");
      } catch (SQLException ignore) {
      }
      throw e;
    }
  }

  private Connection getConnectionEnsureExists(
      TileCoordinates tile, boolean acquireMutexOnCreate, boolean readOnly) throws IOException {
    Path path = getTilesetPath(tile.getLevel(), tile.getRow(), tile.getCol());

    // ensure that the file exists
    if (!Files.exists(path)) {
      createMbtilesFile(path, acquireMutexOnCreate);
    }

    return SqlHelper.getConnection(path, readOnly);
  }

  private Optional<Connection> getConnectionIfExists(TileCoordinates tile, boolean readOnly) {
    return getConnectionIfExists(tile.getLevel(), tile.getRow(), tile.getCol(), readOnly);
  }

  private Optional<Connection> getConnectionIfExists(
      int level, int row, int col, boolean readOnly) {
    Path path = getTilesetPath(level, row, col);

    if (!Files.exists(path)) {
      return Optional.empty();
    }

    return Optional.of(SqlHelper.getConnection(path, readOnly));
  }

  public String getStorageInfo(int level, int row, int col) {
    return getTilesetPath(level, row, col).toString();
  }

  private void createMbtilesFile(Path path, boolean acquireMutexOnCreate) throws IOException {
    // acquire the mutex, if necessary (for write operations we already have it)
    boolean acquired = false;
    Connection connection = null;
    try {
      acquired = acquireMutexOnCreate && mutex.tryAcquire(5, TimeUnit.SECONDS);
      if (acquireMutexOnCreate)
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("getConnection: Trying to acquire mutex: '{}'.", acquired);
        }
      if (acquireMutexOnCreate && !acquired)
        throw new IllegalStateException(
            String.format("Could not acquire mutex to create MBTiles file: %s", path));
      // now that we have the mutex, check again, if the file exists, it may have been
      // created by a parallel request
      if (!Files.exists(path)) {
        // recreate an empty MBTiles container
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("Creating MBTiles file '{}'.", path);
        }
        Files.createDirectories(path.getParent());
        connection = SqlHelper.getConnection(path, false);
        try {
          initMbtilesDb(metadata, connection);
        } catch (SQLException | IOException e) {
          throw new IllegalStateException(
              String.format("Could not create new Mbtiles file: %s", path), e);
        }
      }
    } catch (InterruptedException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("getConnection: Thread has been interrupted.");
      }
    } finally {
      try {
        releaseConnection(connection);
      } catch (SQLException ignore) {
      }
      if (acquired) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("getConnection: Releasing mutex.");
        }
        mutex.release();
      }
    }
  }

  private void releaseConnection(@Nullable Connection connection) throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  public MbtilesMetadata getMetadata() throws SQLException, IOException {
    Optional<Connection> optionalConnection = getConnectionIfExists(0, 0, 0, true);

    if (optionalConnection.isEmpty()) {
      throw new IOException("file does not exist");
    }

    ImmutableMbtilesMetadata.Builder builder = ImmutableMbtilesMetadata.builder();
    String sql = "SELECT name, value FROM metadata";
    try (Connection connection = optionalConnection.get();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      while (rs.next()) {
        final String name = rs.getString("name");
        final String value = rs.getString("value");
        if (Objects.nonNull(value)) {
          switch (name) {
            case "name":
              builder.name(value);
              break;
            case "format":
              TilesFormat format = TilesFormat.of(value);
              if (Objects.isNull(format))
                throw new IllegalArgumentException(
                    String.format(
                        "The metadata entry '%s' in an Mbtiles container has an invalid value '%s'",
                        name, value));
              builder.format(format);
              break;
            case "bounds":
              List<Double> bounds =
                  Splitter.on(',')
                      .trimResults()
                      .omitEmptyStrings()
                      .splitToStream(value)
                      .map(Double::parseDouble)
                      .collect(Collectors.toUnmodifiableList());
              if (bounds.size() != 4)
                throw new IllegalArgumentException(
                    String.format(
                        "The metadata entry '%s' in an Mbtiles container has an invalid value '%s'",
                        name, value));
              builder.bounds(bounds);
              break;
            case "center":
              List<Double> center =
                  Splitter.on(',')
                      .trimResults()
                      .omitEmptyStrings()
                      .splitToStream(value)
                      .map(Double::parseDouble)
                      .collect(Collectors.toUnmodifiableList());
              if (center.size() != 3)
                throw new IllegalArgumentException(
                    String.format(
                        "The metadata entry '%s' in an Mbtiles container has an invalid value '%s'",
                        name, value));
              builder.center(center);
              break;
            case "minzoom":
              builder.minzoom(Integer.parseInt(value));
              break;
            case "maxzoom":
              builder.maxzoom(Integer.parseInt(value));
              break;
            case "description":
              builder.description(value);
              break;
            case "attribution":
              builder.attribution(value);
              break;
            case "type":
              MbtilesMetadata.MbtilesType type = MbtilesMetadata.MbtilesType.of(value);
              if (Objects.isNull(type))
                throw new IllegalArgumentException(
                    String.format(
                        "The metadata entry '%s' in an Mbtiles container has an invalid value '%s'",
                        name, value));
              builder.type(type);
              break;
            case "version":
              try {
                int v = Integer.parseInt(value);
                builder.version(v);
              } catch (NumberFormatException e) {
                builder.version(Float.parseFloat(value));
              }
              break;
            case "json":
              ObjectMapper mapper = new ObjectMapper();
              try {
                ArrayNode layers = (ArrayNode) mapper.readTree(value).get("vector_layers");
                for (JsonNode node : layers) {
                  ObjectNode layer = (ObjectNode) node;
                  ImmutableVectorLayer.Builder builder2 =
                      ImmutableVectorLayer.builder().id(layer.get("id").asText());
                  if (layer.has("description")) {
                    builder2.description(layer.get("description").asText());
                  }
                  if (layer.has("minzoom")) {
                    builder2.minzoom(layer.get("minzoom").asDouble());
                  }
                  if (layer.has("maxzoom")) {
                    builder2.minzoom(layer.get("maxzoom").asDouble());
                  }
                  ObjectNode fields = (ObjectNode) layer.get("fields");
                  for (Iterator<Entry<String, JsonNode>> it = fields.fields(); it.hasNext(); ) {
                    Entry<String, JsonNode> field = it.next();
                    builder2.putFields(field.getKey(), field.getValue().textValue());
                  }
                  builder.addVectorLayers(builder2.build());
                }
              } catch (IOException e) {
                if (LOGGER.isErrorEnabled()) {
                  LOGGER.error(
                      "Could not parse Vector Layers object from MBTiles metadata, the vector layers are ignored: {}",
                      e.getMessage());
                }
                if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
                  LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
                }
              }
              break;
          }
        }
      }
    }

    return builder.build();
  }

  public Optional<InputStream> getTile(TileQuery tile) throws SQLException, IOException {
    Optional<Connection> optionalConnection = getConnectionIfExists(tile, true);

    if (optionalConnection.isEmpty()) {
      return Optional.empty();
    }

    Optional<InputStream> result = Optional.empty();
    int level = tile.getLevel();
    int tmsRow = tile.getTileMatrixSet().getTmsRow(level, tile.getRow());
    int col = tile.getCol();
    boolean gzip = Objects.equals(tile.getMediaType(), FeatureEncoderMVT.FORMAT);
    String sql =
        String.format(
            "SELECT tile_data FROM tiles WHERE zoom_level=%d AND tile_row=%d AND tile_column=%d",
            level, tmsRow, col);
    try (Connection connection = optionalConnection.get();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      if (rs.next()) {
        result =
            Optional.of(
                gzip
                    ? new GZIPInputStream(rs.getBinaryStream("tile_data"))
                    : rs.getBinaryStream("tile_data"));
      }
    }
    return result;
  }

  public Optional<Boolean> tileIsEmpty(TileCoordinates tile) throws SQLException, IOException {
    Optional<Connection> optionalConnection = getConnectionIfExists(tile, true);

    if (optionalConnection.isEmpty()) {
      return Optional.empty();
    }

    Optional<Boolean> result = Optional.empty();
    int level = tile.getLevel();
    int tmsRow = tile.getTileMatrixSet().getTmsRow(level, tile.getRow());
    int col = tile.getCol();
    String sql =
        String.format(
            "SELECT tile_id FROM tiles WHERE zoom_level=%d AND tile_row=%d AND tile_column=%d",
            level, tmsRow, col);
    try (Connection connection = optionalConnection.get();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      if (rs.next()) {
        result = Optional.of(rs.getInt("tile_id") == EMPTY_TILE_ID);
      }
    }
    return result;
  }

  @FunctionalInterface
  public interface Walker {
    void walk(int level, int row, int col);
  }

  public void walk(Walker walker) throws SQLException, IOException {
    if (partitions.isEmpty()) {
      walk(0, 0, 0, walker);
      return;
    }

    for (Path path : getTilesetPaths()) {
      int[] partitionScope =
          partitions
              .get()
              .getPartitionScope(path.getFileName().toString().replace(MBTILES_SUFFIX, ""));

      walk(partitionScope[0], partitionScope[1], partitionScope[3], walker);
    }
  }

  private void walk(int level, int row, int col, Walker walker) throws SQLException, IOException {
    Optional<Connection> optionalConnection = getConnectionIfExists(level, row, col, true);

    if (optionalConnection.isEmpty()) {
      return;
    }

    // we need to close this result set, before we start to walk;
    // first determine the number of tiles to process and store the tile coordinates in an array
    String sql = "SELECT COUNT(zoom_level) FROM tiles";
    int[][] tiles = new int[0][3];
    int numTiles = 0;
    try (Connection connection = optionalConnection.get()) {
      try (Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(sql)) {
        if (rs.next()) {
          numTiles = rs.getInt(1);
        }
      }
      tiles = new int[numTiles][3];
      int i = 0;
      sql = "SELECT zoom_level, tile_row, tile_column FROM tiles";
      try (Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(sql)) {
        while (rs.next()) {
          tiles[i][0] = rs.getInt(1);
          tiles[i][1] = rs.getInt(2);
          tiles[i++][2] = rs.getInt(3);
        }
      }
    } catch (SQLException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Walker could not be initialized. Reason: {}", e.getMessage());
      }
      if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
        LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
      }
      throw e;
    }

    // the connection is closed, we can start to walk
    for (int i = 0; i < numTiles; i++) {
      walker.walk(tiles[i][0], tiles[i][1], tiles[i][2]);
    }
  }

  public boolean tileExists(TileCoordinates tile) throws SQLException, IOException {
    int level = tile.getLevel();
    int row = tile.getRow();
    int tmsRow = tile.getTileMatrixSet().getTmsRow(level, tile.getRow());
    int col = tile.getCol();

    return tileExists(level, row, tmsRow, col);
  }

  public boolean tileExists(int level, int row, int tmsRow, int col)
      throws SQLException, IOException {
    Optional<Connection> optionalConnection = getConnectionIfExists(level, row, col, true);

    if (optionalConnection.isEmpty()) {
      return false;
    }

    String sql =
        String.format(
            "SELECT tile_data FROM tiles WHERE zoom_level=%d AND tile_row=%d AND tile_column=%d",
            level, tmsRow, col);
    boolean exists;
    try (Connection connection = optionalConnection.get();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      exists = rs.next();
    } catch (SQLException e) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(
            "Could not determine existence of TMS tile {}/{}/{}. Query: {}.",
            level,
            tmsRow,
            col,
            sql);
      }
      throw e;
    }
    return exists;
  }

  public boolean hasAnyTiles() throws SQLException, IOException {
    if (partitions.isEmpty()) {
      return hasAnyTiles(0, 0, 0);
    }

    boolean hasAnyTiles = false;

    for (Path path : getTilesetPaths()) {
      int[] partitionScope =
          partitions
              .get()
              .getPartitionScope(path.getFileName().toString().replace(MBTILES_SUFFIX, ""));
      hasAnyTiles |= hasAnyTiles(partitionScope[0], partitionScope[1], partitionScope[3]);
    }

    return hasAnyTiles;
  }

  private boolean hasAnyTiles(int level, int row, int col) throws SQLException, IOException {
    Optional<Connection> optionalConnection = getConnectionIfExists(level, row, col, true);

    if (optionalConnection.isEmpty()) {
      return false;
    }

    String sql = String.format("SELECT COUNT(*) FROM %s", tileBlobsTable);
    long count;
    try (Connection connection = optionalConnection.get();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      count = rs.getLong(1);
    }
    return count > 1;
  }

  public void writeTile(TileQuery tile, byte[] content) throws SQLException, IOException {
    int level = tile.getLevel();
    int tmsRow = tile.getTileMatrixSet().getTmsRow(level, tile.getRow());
    int col = tile.getCol();
    boolean gzip = Objects.equals(tile.getMediaType(), FeatureEncoderMVT.FORMAT);
    boolean supportsEmptyTile = Objects.equals(tile.getMediaType(), FeatureEncoderMVT.FORMAT);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Write tile {}/{}/{}/{} to MBTiles cache {}.",
          tile.getTileMatrixSet().getId(),
          level,
          tile.getRow(),
          col,
          getTilesetPath(tile));
    }
    Connection connection = null;
    boolean acquired = false;
    try {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("writeTile: Trying to acquire mutex: '{}'.", acquired);
      }
      acquired = mutex.tryAcquire(5, TimeUnit.SECONDS);
      if (acquired) {
        connection = getConnectionEnsureExists(tile, false, false);
        SqlHelper.execute(connection, "BEGIN IMMEDIATE");
        // do we have an old blob?
        boolean exists = false;
        Integer old_tile_id = null;
        String sql =
            String.format(
                "SELECT tile_id FROM tile_map WHERE zoom_level=%d AND tile_row=%d AND tile_column=%d",
                level, tmsRow, col);
        try (Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql)) {
          if (rs.next()) {
            exists = true;
            old_tile_id = rs.getInt(1);
          }
        }
        // add the new tile
        int tile_id = EMPTY_TILE_ID;
        if (content.length > 0 || !supportsEmptyTile) {
          try (PreparedStatement statement =
              connection.prepareStatement("INSERT INTO tile_blobs (tile_data) VALUES(?)")) {
            ByteArrayOutputStream mvt = new ByteArrayOutputStream(content.length);
            if (gzip) {
              GZIPOutputStream gzipStream = new GZIPOutputStream(mvt);
              gzipStream.write(content);
              gzipStream.close();
            } else {
              mvt.write(content);
            }
            statement.setBytes(1, mvt.toByteArray());
            statement.executeUpdate();
          }
          sql = "SELECT last_insert_rowid()";
          try (Statement statement = connection.createStatement();
              ResultSet rs = statement.executeQuery(sql)) {
            tile_id = rs.getInt(1);
          }
        }
        sql =
            exists
                ? "UPDATE tile_map SET tile_id=? WHERE zoom_level=? AND tile_row=? AND tile_column=?"
                : "INSERT INTO tile_map (tile_id,zoom_level,tile_row,tile_column) VALUES(?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
          statement.setInt(1, tile_id);
          statement.setInt(2, level);
          statement.setInt(3, tmsRow);
          statement.setInt(4, col);
          statement.executeUpdate();
        }

        // finally remove any old blob
        if (Objects.nonNull(old_tile_id) && (old_tile_id != EMPTY_TILE_ID || !supportsEmptyTile)) {
          sql = "DELETE FROM tile_map WHERE tile_id=?";
          try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, old_tile_id);
            statement.executeUpdate();
          }
        }

        SqlHelper.execute(connection, "COMMIT");
      }
    } catch (SQLException e) {
      try {
        SqlHelper.execute(connection, "ROLLBACK");
      } catch (Exception ignore) {
      }
      throw e;
    } catch (InterruptedException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("writeTile: Thread has been interrupted.");
      }
    } finally {
      releaseConnection(connection);
      if (acquired) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("writeTile: Releasing mutex.");
        }
        mutex.release();
      }
    }
  }

  public void deleteTile(TileQuery tile) throws SQLException, IOException {
    int level = tile.getLevel();
    int row = tile.getRow();
    int tmsRow = tile.getTileMatrixSet().getTmsRow(level, tile.getRow());
    int col = tile.getCol();
    boolean supportsEmtpyTile = Objects.equals(tile.getMediaType(), FeatureEncoderMVT.FORMAT);

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Delete tile {}/{}/{}/{} from MBTiles cache {}.",
          tile.getTileMatrixSet().getId(),
          level,
          row,
          col,
          getTilesetPath(tile));
    }

    deleteTile(level, row, tmsRow, col, supportsEmtpyTile);
  }

  public void deleteTile(int level, int row, int tmsRow, int col, boolean supportsEmptyTile)
      throws SQLException, IOException {
    Connection connection = null;
    boolean acquired = false;
    try {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("deleteTile: Trying to acquire mutex: '{}'.", acquired);
      }
      acquired = mutex.tryAcquire(5, TimeUnit.SECONDS);
      if (acquired) {
        Optional<Connection> optionalConnection = getConnectionIfExists(level, row, col, false);

        if (optionalConnection.isEmpty()) {
          return;
        }

        connection = optionalConnection.get();
        String sql =
            String.format(
                "SELECT tile_id FROM %s WHERE zoom_level=%d AND tile_row=%d AND tile_column=%d",
                tileMapTable, level, tmsRow, col);

        if (numericTileIds) {
          int tile_id = Integer.MIN_VALUE;
          try (Statement statement = connection.createStatement();
              ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
              tile_id = rs.getInt(1);
            }
          }
          sql =
              String.format(
                  "DELETE FROM %s WHERE zoom_level=%d AND tile_row=%d AND tile_column=%d",
                  tileMapTable, level, tmsRow, col);
          SqlHelper.execute(connection, sql);
          if (tile_id != Integer.MIN_VALUE && (tile_id != EMPTY_TILE_ID || !supportsEmptyTile)) {
            sql = String.format("DELETE FROM %s WHERE tile_id=%d", tileBlobsTable, tile_id);
            SqlHelper.execute(connection, sql);
          }
        } else {
          String tile_id = "";
          try (Statement statement = connection.createStatement();
              ResultSet rs = statement.executeQuery(sql)) {
            if (rs.next()) {
              tile_id = rs.getString(1);
            }
          }
          LOGGER.debug("DELETE Tile id: '{}'", tile_id);
          sql = String.format("DELETE FROM %s WHERE tile_id='%s'", tileMapTable, tile_id);
          SqlHelper.execute(connection, sql);
          sql = String.format("DELETE FROM %s WHERE tile_id='%s'", tileBlobsTable, tile_id);
          SqlHelper.execute(connection, sql);
        }
      }
    } catch (SQLException e) {
      try {
        SqlHelper.execute(connection, "ROLLBACK");
      } catch (Exception ignore) {
      }
      throw e;
    } catch (InterruptedException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("deleteTile: Thread has been interrupted.");
      }
    } finally {
      releaseConnection(connection);
      if (acquired) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("deleteTile: Releasing mutex.");
        }
        mutex.release();
      }
    }
  }

  public void deleteTiles(TileMatrixSetBase tileMatrixSet, TileMatrixSetLimits limits)
      throws SQLException, IOException {
    int level = Integer.parseInt(limits.getTileMatrix());

    if (partitions.isEmpty()) {
      deleteTiles(
          tileMatrixSet,
          level,
          limits.getMinTileRow(),
          limits.getMaxTileRow(),
          limits.getMinTileCol(),
          limits.getMaxTileCol());
      return;
    }

    for (TileSubMatrix subMatrix : partitions.get().getSubMatrices(limits)) {
      deleteTiles(
          tileMatrixSet,
          level,
          subMatrix.getRowMin(),
          subMatrix.getRowMax(),
          subMatrix.getColMin(),
          subMatrix.getColMax());
    }
  }

  public void deleteTiles(
      TileMatrixSetBase tileMatrixSet, int level, int minRow, int maxRow, int minCol, int maxCol)
      throws SQLException, IOException {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Delete tiles {}/{}/*/* from MBTiles cache {}.",
          tileMatrixSet.getId(),
          level,
          getTilesetPath(level, minRow, minCol));
    }
    Connection connection = null;
    boolean acquired = false;
    try {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("deleteTiles: Trying to acquire mutex: '{}'.", acquired);
      }
      acquired = mutex.tryAcquire(5, TimeUnit.SECONDS);
      if (!acquired)
        throw new IllegalStateException(
            String.format(
                "Could not acquire mutex to delete tiles in MBTiles file: %s",
                getTilesetPath(null)));

      Optional<Connection> optionalConnection = getConnectionIfExists(level, minRow, minCol, false);

      if (optionalConnection.isEmpty()) {
        return;
      }

      connection = optionalConnection.get();
      String sqlFrom =
          String.format(
              "FROM %s WHERE zoom_level=%d AND tile_row>=%d AND tile_column>=%d AND tile_row<=%d AND tile_column<=%d",
              tileMapTable,
              level,
              tileMatrixSet.getTmsRow(level, maxRow),
              minCol,
              tileMatrixSet.getTmsRow(level, minRow),
              maxCol);
      String sql = String.format("SELECT DISTINCT tile_id %s", sqlFrom);
      ArrayList<Integer> tile_ids = new ArrayList<>();
      try (Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(sql)) {
        while (rs.next()) {
          int tile_id = rs.getInt(1);
          if (tile_id != EMPTY_TILE_ID) {
            tile_ids.add(tile_id);
          }
        }
      } catch (SQLException e) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Could not determine tiles to be deleted. Query: {}. Reason: {}",
              sql,
              e.getMessage());
        }
        if (LOGGER.isDebugEnabled(LogContext.MARKER.STACKTRACE)) {
          LOGGER.debug(LogContext.MARKER.STACKTRACE, "Stacktrace: ", e);
        }
      }
      SqlHelper.execute(connection, "BEGIN IMMEDIATE");
      int idx = 0;
      while (idx < tile_ids.size()) {
        String sqlDeleteBlobs =
            String.format(
                "DELETE FROM %s WHERE tile_id IN (%s)",
                tileBlobsTable,
                tile_ids.subList(idx, Math.min(idx + IDS_CHUCK_SIZE, tile_ids.size())).stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(",")));
        SqlHelper.execute(connection, sqlDeleteBlobs);
        idx += IDS_CHUCK_SIZE;
      }
      SqlHelper.execute(connection, String.format("DELETE %s", sqlFrom));
      SqlHelper.execute(connection, "COMMIT");
    } catch (SQLException e) {
      try {
        SqlHelper.execute(connection, "ROLLBACK");
      } catch (Exception ignore) {
      }
      throw e;
    } catch (InterruptedException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("deleteTiles: Thread has been interrupted.");
      }
    } finally {
      releaseConnection(connection);
      if (acquired) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("deleteTiles: Releasing mutex.");
        }
        mutex.release();
      }
    }
  }

  public void cleanup() throws SQLException, IOException {
    if (partitions.isEmpty()) {
      cleanup(0, 0, 0);
      return;
    }

    for (Path path : getTilesetPaths()) {
      int[] partitionScope =
          partitions
              .get()
              .getPartitionScope(path.getFileName().toString().replace(MBTILES_SUFFIX, ""));

      cleanup(partitionScope[0], partitionScope[1], partitionScope[3]);
    }
  }

  private void cleanup(int level, int row, int col) throws SQLException, IOException {
    Connection connection = null;
    boolean acquired = false;
    try {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("cleanup: Trying to acquire mutex: '{}'.", acquired);
      }
      acquired = mutex.tryAcquire(5, TimeUnit.SECONDS);
      if (acquired) {
        Optional<Connection> optionalConnection = getConnectionIfExists(level, row, col, false);

        if (optionalConnection.isEmpty()) {
          return;
        }

        connection = optionalConnection.get();
        SqlHelper.execute(connection, "VACUUM");
      }
    } catch (InterruptedException e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("cleanup: Thread has been interrupted.");
      }
    } finally {
      releaseConnection(connection);
      if (acquired) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("cleanup: Releasing mutex.");
        }
        mutex.release();
      }
    }
  }
}
