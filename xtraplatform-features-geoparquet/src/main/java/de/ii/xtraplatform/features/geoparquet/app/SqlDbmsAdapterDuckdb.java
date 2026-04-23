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

  /**
   * Debug-logs when two file paths share the same file name.
   *
   * @param filePath The first file path
   * @param otherFilePath The second file path
   */
  private void logSharedFilename(Path filePath, Path otherFilePath) {
    if (filePath.getFileName().equals(otherFilePath.getFileName())) {
      LOGGER.debug(
          "The following files share the same file name: {} and {}", filePath, otherFilePath);
    }
  }

  /**
   * Adds a query to create an in-memory table containing the content of the parquet file found
   * under filePath. The name of the new table equals to the filePath with every '/' replaced with
   * '__'. This is done to make sure every table name is unique even when multiple files in
   * different directories share the same name. The choice of replacing the slashes with '__' has
   * been made to avoid collisions with files whose names contain underscores (A/a.parquet and
   * A_a.parquet would collide).
   *
   * @param queryBuilder The StringBuilder of the init query.
   * @param filePath The file path of the parquet file to add.
   * @param addedFilePaths List containing every file path added yet. Necessary to log files that
   *     share the same file name.
   */
  private void addTable(StringBuilder queryBuilder, Path filePath, List<Path> addedFilePaths) {
    addedFilePaths.forEach(otherFilePath -> logSharedFilename(filePath, otherFilePath));
    addedFilePaths.add(filePath);

    queryBuilder.append(
        String.format(
            "CREATE TABLE '%s' AS SELECT * FROM '%s';",
            filePath.toString().replace("/", "__").replace(".parquet", ""), filePath));
  }

  private Optional<String> handleLocalFiles(
      StringBuilder queryBuilder, ConnectionInfoSql connectionInfo) {
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
    queryBuilder.append(String.format("SET file_search_path = '%s';", dataDirectory));

    // Walk the tree of the data directory and generate a query to add the content of every parquet
    // file as an in-memory table. To increase the collision resistance, '/' is replaced with '__'
    // instead of '_'.
    List<Path> addedFilePaths = new ArrayList<>();
    try (Stream<Path> pathStream = Files.walk(dataDirectory)) {
      pathStream
          .filter(Files::isRegularFile)
          .filter(filePath -> filePath.getFileName().toString().endsWith(".parquet"))
          .map(dataDirectory::relativize)
          .forEach(filePath -> addTable(queryBuilder, filePath, addedFilePaths));
    } catch (IOException e) {
      throw new IllegalStateException("Error traversing the data directory: " + e.getMessage());
    }

    return Optional.of(queryBuilder.toString());
  }

  // Todo: Create custom ConnectionInfo for GeoParquet
  private Optional<String> handleS3(StringBuilder queryBuilder, ConnectionInfoSql connectionInfo) {
    // Install and load the httpfs extension of DuckDB to access files via S3
    queryBuilder.append("INSTALL httpfs;");
    queryBuilder.append("LOAD httpfs;");

    // Add region
    queryBuilder.append(String.format("SET s3_region='%s';", connectionInfo.getDatabase()));

    // Add credentials if avaible
    connectionInfo
        .getUser()
        .ifPresent(key -> queryBuilder.append(String.format("SET s3_access_key_id='%s';", key)));
    connectionInfo
        .getPassword()
        .ifPresent(
            secret -> queryBuilder.append(String.format("SET s3_secret_access_key='%s';", secret)));

    Map<String, String> s3Mapping = connectionInfo.getDriverOptions();
    for (var key : s3Mapping.keySet()) {
      queryBuilder.append(
          String.format("CREATE TABLE '%s' AS SELECT * FROM '%s';", key, s3Mapping.get(key)));
    }

    return Optional.of(queryBuilder.toString());
  }

  @Override
  public Optional<String> getInitSql(ConnectionInfoSql connectionInfo) {
    StringBuilder queryBuilder = new StringBuilder(512);

    // Install and load the spatial extension of DuckDB to access spatial functions
    queryBuilder.append("INSTALL spatial;");
    queryBuilder.append("LOAD spatial;");

    // Data is retrieved from S3
    if (connectionInfo.getHost().isPresent() && connectionInfo.getHost().get().equals("s3"))
      return handleS3(queryBuilder, connectionInfo);

    // Data is avaible as local GeoParquet files
    return handleLocalFiles(queryBuilder, connectionInfo);
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
