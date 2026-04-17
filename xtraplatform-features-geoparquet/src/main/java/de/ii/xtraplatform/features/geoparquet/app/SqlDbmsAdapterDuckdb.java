/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.geoparquet.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import org.davidmoten.rxjava3.jdbc.pool.DatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class SqlDbmsAdapterDuckdb implements SqlDbmsAdapter {

  static final String ID = "DUCKDB";

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlDbmsAdapterDuckdb.class);

  private final ResourceStore featuresStore;
  private final String applicationName;
  private final SqlDialect dialect;

  @Inject
  public SqlDbmsAdapterDuckdb(AppContext appContext, ResourceStore resourceStore) {
    this.featuresStore = resourceStore.with("features");
    this.applicationName =
        String.format("%s %s - %%s", appContext.getName(), appContext.getVersion());
    this.dialect = new SqlDialectDuckdb();
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public SqlDialect getDialect() {
    return dialect;
  }

  @Override
  public DataSource createDataSource(String providerId, ConnectionInfoSql connectionInfo) {
    return new DuckdbDataSource();
  }

  private void logCollision(Path filePath, Path otherFilePath) {
    if (filePath.getFileName().equals(otherFilePath.getFileName())) {
      LOGGER.debug("Parquet file name collision: {} with {}", filePath, otherFilePath);
    }
  }

  private void addTable(StringBuilder query, Path filePath, List<Path> addedFilePaths) {
    addedFilePaths.forEach(otherFilePath -> logCollision(filePath, otherFilePath));
    addedFilePaths.add(filePath);

    query.append(
        String.format(
            "CREATE TABLE '%s' AS SELECT * FROM '%s';",
            filePath.toString().replace("/", "__").replace(".parquet", ""), filePath));
  }

  @Override
  public Optional<String> getInitSql(ConnectionInfoSql connectionInfo) {
    StringBuilder query = new StringBuilder(512);

    // Install and load the spatial extension of DuckDB to access spatial functions
    query.append("INSTALL spatial;");
    query.append("LOAD spatial;");

    // Find absolute Path to resources/features, inspired from SqlDbmsAdapterGpkg.java
    Optional<Path> featuresPath = Optional.empty();
    try {
      featuresPath = featuresStore.asLocalPath(Path.of(""), false);
    } catch (IOException e) {
      // continue
    }
    if (featuresPath.isEmpty()) {
      throw new IllegalStateException("Creation of absolute path to resources/features failed.");
    }

    // Make Path-Object to the data directory
    final Path dataDirectory = featuresPath.get().resolve(connectionInfo.getDatabase());
    if (!Files.exists(dataDirectory)) {
      throw new IllegalStateException("Directory does not exist: " + dataDirectory);
    }
    if (!Files.isDirectory(dataDirectory)) {
      throw new IllegalStateException("Is not a directory: " + dataDirectory);
    }

    // Set the absolute path to the data directory as the search path
    query.append(String.format("SET file_search_path = '%s';", dataDirectory));

    // Walk the tree of the data directory and generate a query to add the content of every parquet
    // file as an in-memory table. To increase the collision resistance, '/' is replaced with '__'
    // instead of '_'.
    List<Path> addedFilePaths = new ArrayList<>();
    try (Stream<Path> pathStream = Files.walk(dataDirectory)) {
      pathStream
          .filter(Files::isRegularFile)
          .filter(filePath -> filePath.getFileName().toString().endsWith(".parquet"))
          .map(dataDirectory::relativize)
          .forEach(filePath -> addTable(query, filePath, addedFilePaths));
    } catch (IOException e) {
      throw new IllegalStateException("Error traversing the data directory: " + e.getMessage());
    }

    return Optional.of(query.toString());
  }

  @Override
  public List<String> getDefaultSchemas() {
    return List.of();
  }

  @Override
  public DatabaseType getRxType() {
    return DatabaseType.OTHER;
  }

  @Override
  public List<String> getSystemSchemas() {
    return List.of("public");
  }

  @Override
  public List<String> getSystemTables() {
    return List.of();
  }

  @Override
  public Map<String, GeoInfo> getGeoInfo(Connection connection, DbInfo dbInfo) throws SQLException {
    return Map.of();
  }

  @Override
  public DbInfo getDbInfo(Connection connection) throws SQLException {
    return new DbInfo() {};
  }

  @Override
  public Collator getRowSortingCollator(Optional<String> defaultCollation) {
    return null;
  }
}
