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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import org.davidmoten.rxjava3.jdbc.pool.DatabaseType;

@Singleton
@AutoBind
public class SqlDbmsAdapterDuckdb implements SqlDbmsAdapter {

  static final String ID = "DUCKDB";

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

  @Override
  public Optional<String> getInitSql(ConnectionInfoSql connectionInfo) {
    StringBuilder query = new StringBuilder(256);

    // Install and load the spatial extension of DuckDB to access spatial functions
    query.append("INSTALL spatial;");
    query.append("LOAD spatial;");

    // Create absolute Path to the data directory, inspired from SqlDbmsAdapterGpkg.java
    Optional<Path> absoluteFeaturesPath = Optional.empty();
    try {
      absoluteFeaturesPath = featuresStore.asLocalPath(Path.of(""), false);
    } catch (IOException e) {
      // continue
    }
    if (absoluteFeaturesPath.isEmpty()) {
      throw new IllegalStateException("Creation of absolute path to resources/features failed.");
    }

    // Set the absolute path of resources/features as the search-path
    query.append(String.format("SET file_search_path = '%s';", absoluteFeaturesPath.get()));

    // Create tables in-memory to access them with their name instead of a 'read_parquet(PATH)'
    // statement
    for (int i = 0; i < connectionInfo.getSchemas().size(); i++) {
      String parquetFile = connectionInfo.getSchemas().get(i);

      if (!Files.exists(absoluteFeaturesPath.get().resolve(parquetFile + ".parquet"))) {
        throw new IllegalStateException("Parquet file not found: " + parquetFile + ".parquet");
      } else {
        query.append(
            String.format(
                "CREATE TABLE %s AS SELECT * FROM '%s.parquet';", parquetFile, parquetFile));
      }
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
