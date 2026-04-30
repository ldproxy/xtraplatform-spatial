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

  // Constants to extract parameter from driverOptions
  private static final String TABLE_PREFIX = "table.";
  private static final String DRIVER_OPT_TYPE = "type";
  private static final String DRIVER_OPT_ENDPOINT = "endpoint";
  private static final String DRIVER_OPT_REGION = "region";
  private static final String DRIVER_OPT_SESSION_TOKEN = "session_token";
  private static final String DRIVER_OPT_URL_COMPATIBILITY_MODE = "url_compatibility_mode";
  private static final String DRIVER_OPT_URL_STYLE = "url_style";
  private static final String DRIVER_OPT_USE_SSL = "use_ssl";
  private static final String DRIVER_OPT_VERIFY_SSL = "verify_ssl";
  private static final String DRIVER_OPT_ACCOUNT_ID = "account_id";
  private static final String DRIVER_OPT_KMS_KEY_ID = "kms_key_id";
  private static final String DRIVER_OPT_REQUESTER_PAYS = "requester_pays";

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

    // Extract all parameter necessary driver parameter, see
    // https://duckdb.org/docs/current/core_extensions/httpfs/s3api#overview-of-s3-secret-parameters
    // ToDo: Check if the input is valid
    ArrayList<String> driverParameter = new ArrayList<>();
    Map<String, String> driverOptions = connectionInfo.getDriverOptions();

    // Extract ENDPOINT
    if (driverOptions.containsKey(DRIVER_OPT_ENDPOINT)) {
      driverParameter.add(String.format("ENDPOINT '%s'", driverOptions.get(DRIVER_OPT_ENDPOINT)));
    }

    // Extract KEY_ID
    connectionInfo
        .getUser()
        .ifPresent(key -> driverParameter.add(String.format("KEY_ID '%s'", key)));

    // Extract REGION
    if (driverOptions.containsKey(DRIVER_OPT_REGION)) {
      driverParameter.add(String.format("REGION '%s'", driverOptions.get(DRIVER_OPT_REGION)));
    }

    // Extract SECRET
    connectionInfo
        .getPassword()
        .ifPresent(secret -> driverParameter.add(String.format("SECRET '%s'", secret)));

    // Extract SESSION_TOKEN
    if (driverOptions.containsKey(DRIVER_OPT_SESSION_TOKEN)) {
      driverParameter.add(
          String.format("SESSION_TOKEN '%s'", driverOptions.get(DRIVER_OPT_SESSION_TOKEN)));
    }

    // Extract URL_COMPATIBILITY_MODE
    if (driverOptions.containsKey(DRIVER_OPT_URL_COMPATIBILITY_MODE)) {
      driverParameter.add(
          String.format(
              "URL_COMPATIBILITY_MODE %s", driverOptions.get(DRIVER_OPT_URL_COMPATIBILITY_MODE)));
    }

    // Extract URL_STYLE
    if (driverOptions.containsKey(DRIVER_OPT_URL_STYLE)) {
      driverParameter.add(String.format("URL_STYLE '%s'", driverOptions.get(DRIVER_OPT_URL_STYLE)));
    }

    // Extract USE_SSL
    if (driverOptions.containsKey(DRIVER_OPT_USE_SSL)) {
      driverParameter.add(String.format("USE_SSL %s", driverOptions.get(DRIVER_OPT_USE_SSL)));
    }

    // Extract VERIFY_SSL
    if (driverOptions.containsKey(DRIVER_OPT_VERIFY_SSL)) {
      driverParameter.add(String.format("VERIFY_SSL %s", driverOptions.get(DRIVER_OPT_VERIFY_SSL)));
    }

    // Extract ACCOUNT_ID
    if (driverOptions.containsKey(DRIVER_OPT_ACCOUNT_ID)) {
      driverParameter.add(
          String.format("ACCOUNT_ID '%s'", driverOptions.get(DRIVER_OPT_ACCOUNT_ID)));
    }

    // Extract KMS_KEY_ID
    if (driverOptions.containsKey(DRIVER_OPT_KMS_KEY_ID)) {
      driverParameter.add(
          String.format("KMS_KEY_ID '%s'", driverOptions.get(DRIVER_OPT_KMS_KEY_ID)));
    }

    // Extract REQUESTER_PAYS
    if (driverOptions.containsKey(DRIVER_OPT_REQUESTER_PAYS)) {
      driverParameter.add(
          String.format("REQUESTER_PAYS %s", driverOptions.get(DRIVER_OPT_REQUESTER_PAYS)));
    }

    // Create DuckDB Secret containing all the connection parameter
    if (driverOptions.containsKey(DRIVER_OPT_TYPE)) {
      switch (driverOptions.get(DRIVER_OPT_TYPE).toLowerCase()) {
        case "s3" -> queryBuilder.append("CREATE SECRET (TYPE s3");
        case "r2" -> queryBuilder.append("CREATE SECRET (TYPE r2");
        case "gcs" -> queryBuilder.append("CREATE SECRET (TYPE gcs");
        default ->
            throw new IllegalArgumentException(
                "Unknown provider type. Use S3 with a custom endpoint instead.");
      }
    } else {
      queryBuilder.append("CREATE SECRET (TYPE s3");
    }
    if (!driverParameter.isEmpty()) {
      queryBuilder.append(",");
      queryBuilder.append(String.join(",", driverParameter));
    }
    queryBuilder.append(");");

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
    // ToDo: Check if host is valid
    if (connectionInfo.getHost().isPresent()) return handleS3(queryBuilder, connectionInfo);

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
