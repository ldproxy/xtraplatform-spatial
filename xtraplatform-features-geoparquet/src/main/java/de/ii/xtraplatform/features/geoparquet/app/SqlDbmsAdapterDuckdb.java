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

  // ToDo: Log errors
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlDbmsAdapterDuckdb.class);

  // Constants for ConnectionInfo
  private static final String TABLE_PREFIX = "table.";
  private static final String DRIVER_OPT_ENDPOINT = "endpoint";
  private static final String DRIVER_OPT_USE_SSL = "use_ssl";
  private static final String DRIVER_OPT_URL_STYLE = "url_style";

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

  private static void addViews(
      StringBuilder queryBuilder, String prefix, Map<String, String> tableFileMapping) {
    // Add '/' to prefix (bucket or path)
    if (!prefix.isEmpty() && !prefix.endsWith("/")) {
      prefix += "/";
    }

    for (var tableName : tableFileMapping.keySet()) {
      if (tableName.startsWith(TABLE_PREFIX)) {
        queryBuilder.append(
            String.format(
                "CREATE VIEW '%s' AS SELECT * FROM '%s';",
                tableName.substring(TABLE_PREFIX.length()),
                prefix + tableFileMapping.get(tableName)));
      }
    }
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

    addViews(queryBuilder, "", connectionInfo.getDriverOptions());

    return Optional.of(queryBuilder.toString());
  }

  private Optional<String> handleS3(StringBuilder queryBuilder, ConnectionInfoSql connectionInfo) {
    // Install and load the httpfs extension of DuckDB to access files via S3
    queryBuilder.append("INSTALL httpfs;");
    queryBuilder.append("LOAD httpfs;");

    // Extract all parameter necessary to connect to the S3 service
    ArrayList<String> connectingParameter = new ArrayList<>();
    Map<String, String> driverOptions = connectionInfo.getDriverOptions();

    // Extract key
    connectionInfo
        .getUser()
        .ifPresent(key -> connectingParameter.add(String.format("KEY_ID '%s'", key)));

    // Extract secret key
    connectionInfo
        .getPassword()
        .ifPresent(secret -> connectingParameter.add(String.format("SECRET '%s'", secret)));

    // Extract custom endpoint
    if (driverOptions.containsKey(DRIVER_OPT_ENDPOINT)) {
      connectingParameter.add(
          String.format("ENDPOINT '%s'", driverOptions.get(DRIVER_OPT_ENDPOINT)));
    }

    // Extract USE_SLL
    if (driverOptions.containsKey(DRIVER_OPT_USE_SSL)) {
      connectingParameter.add(String.format("USE_SSL %s", driverOptions.get(DRIVER_OPT_USE_SSL)));
    }

    // Extract URL_STYLE
    if (driverOptions.containsKey(DRIVER_OPT_URL_STYLE)) {
      connectingParameter.add(
          String.format("URL_STYLE '%s'", driverOptions.get(DRIVER_OPT_URL_STYLE)));
    }

    // Create DuckDB Secret containing all the connection parameter
    if (!connectingParameter.isEmpty()) {
      queryBuilder.append("CREATE SECRET (TYPE s3,");
      queryBuilder.append(String.join(",", connectingParameter));
      queryBuilder.append(");");
    }

    // Host (bucket) is present because it was checked in getInitSql)
    addViews(queryBuilder, connectionInfo.getHost().get(), driverOptions);

    return Optional.of(queryBuilder.toString());
  }

  @Override
  public Optional<String> getInitSql(ConnectionInfoSql connectionInfo) {
    StringBuilder queryBuilder = new StringBuilder(512);

    // Install and load the spatial extension of DuckDB to access spatial functions
    queryBuilder.append("INSTALL spatial;");
    queryBuilder.append("LOAD spatial;");

    // Data is retrieved from S3
    if (connectionInfo.getHost().isPresent() && connectionInfo.getHost().get().startsWith("s3"))
      return handleS3(queryBuilder, connectionInfo);

    // Data is available as local GeoParquet files
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
