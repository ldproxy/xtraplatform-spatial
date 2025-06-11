/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.EpsgCrs.Force;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import de.ii.xtraplatform.features.sql.domain.ImmutableGeoInfo;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlDialectGpkg;
import de.ii.xtraplatform.features.sql.infra.db.SqlDbmsAdapterGpkg.DbInfoGpkg.SpatialMetadata;
import de.ii.xtraplatform.spatialite.domain.SpatiaLiteLoader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Collator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import org.davidmoten.rxjava3.jdbc.pool.DatabaseType;
import org.immutables.value.Value;
import org.sqlite.SQLiteConnection;
import org.sqlite.SQLiteDataSource;

@Singleton
@AutoBind
public class SqlDbmsAdapterGpkg implements SqlDbmsAdapter {

  public static final String ID = "GPKG";

  private final ResourceStore featuresStore;
  private final SpatiaLiteLoader spatiaLiteLoader;
  private final SqlDialect dialect;

  private boolean spatiaLiteInitialized;

  @Inject
  public SqlDbmsAdapterGpkg(
      AppContext appContext, ResourceStore blobStore, SpatiaLiteLoader spatiaLiteLoader) {
    this.featuresStore = blobStore.with("features");
    this.spatiaLiteLoader = spatiaLiteLoader;
    this.dialect = new SqlDialectGpkg();
    this.spatiaLiteInitialized = false;
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
    Path source = Path.of(connectionInfo.getDatabase());

    if (!source.isAbsolute()) {
      Optional<Path> localPath = Optional.empty();
      try {
        localPath = featuresStore.asLocalPath(source, false);
      } catch (IOException e) {
        // continue
      }
      if (localPath.isPresent()) {
        source = localPath.get();
      } else {
        throw new IllegalStateException("GPKG database not found: " + source);
      }
    } else {
      throw new IllegalStateException(
          "GPKG database reference must be a path relative to resources/features. Found: "
              + source);
    }

    if (!spatiaLiteInitialized && Objects.nonNull(spatiaLiteLoader)) {
      spatiaLiteLoader.load();

      this.spatiaLiteInitialized = true;
    }

    SQLiteDataSource ds =
        new SQLiteDataSource() {
          @Override
          public SQLiteConnection getConnection(String username, String password)
              throws SQLException {
            SQLiteConnection connection = super.getConnection(username, password);

            if (Objects.nonNull(spatiaLiteLoader)) {
              try (var statement = connection.createStatement()) {
                // connection was created a few milliseconds before, so set query timeout is omitted
                // (we assume it will succeed)
                statement.execute(
                    String.format(
                        "SELECT load_extension('%s');", spatiaLiteLoader.getExtensionPath()));
              }
            }

            return connection;
          }
        };

    ds.setLoadExtension(true);
    ds.setUrl(String.format("jdbc:sqlite:%s", source));

    return ds;
  }

  @Override
  public Optional<String> getInitSql(ConnectionInfoSql connectionInfo) {
    return Optional.of("SELECT CASE CheckGeoPackageMetaData() WHEN 1 THEN EnableGpkgMode() END;");
  }

  @Override
  public DatabaseType getRxType() {
    return DatabaseType.SQLITE;
  }

  @Override
  public List<String> getDefaultSchemas() {
    return List.of();
  }

  @Override
  public List<String> getSystemSchemas() {
    return ImmutableList.of();
  }

  @Override
  public List<String> getSystemTables() {
    return ImmutableList.of(
        "gpkg_.*",
        "sqlite_.*",
        "rtree_.*",
        "spatial_ref_sys.*",
        "geometry_columns.*",
        "geom_cols.*",
        "views_geometry_columns.*",
        "virts_geometry_columns.*",
        "vector_layers.*",
        "spatialite_.*",
        "sql_statements_log",
        "sqlite_sequence",
        "ElementaryGeometries",
        "SpatialIndex");
  }

  @Override
  public Map<String, GeoInfo> getGeoInfo(Connection connection, DbInfo dbInfo) throws SQLException {
    if (!(dbInfo instanceof DbInfoGpkg)
        || ((DbInfoGpkg) dbInfo).getSpatialMetadata() == SpatialMetadata.UNSUPPORTED) {
      throw new SQLException("Not a valid spatial SQLite database.");
    }
    String query =
        ((DbInfoGpkg) dbInfo).getSpatialMetadata() == SpatialMetadata.GPKG
            ? String.format(
                "SELECT table_name AS \"%s\", column_name AS \"%s\", CASE z WHEN 1 THEN 3 ELSE 2 END AS \"%s\", srs_id AS \"%s\", geometry_type_name AS \"%s\" FROM gpkg_geometry_columns;",
                GeoInfo.TABLE, GeoInfo.COLUMN, GeoInfo.DIMENSION, GeoInfo.SRID, GeoInfo.TYPE)
            : String.format(
                "SELECT f_table_name AS \"%s\", f_geometry_column AS \"%s\", coord_dimension AS \"%s\", srid AS \"%s\", geometry_type AS \"%s\" FROM geometry_columns;",
                GeoInfo.TABLE, GeoInfo.COLUMN, GeoInfo.DIMENSION, GeoInfo.SRID, GeoInfo.TYPE);

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(query);
    Map<String, GeoInfo> result = new LinkedHashMap<>();

    while (rs.next()) {
      result.put(
          rs.getString(GeoInfo.TABLE),
          ImmutableGeoInfo.of(
              null,
              rs.getString(GeoInfo.TABLE),
              rs.getString(GeoInfo.COLUMN),
              rs.getString(GeoInfo.DIMENSION),
              rs.getString(GeoInfo.SRID),
              forceAxisOrder((DbInfoGpkg) dbInfo).name(),
              rs.getString(GeoInfo.TYPE)));
    }

    return result;
  }

  @Override
  public DbInfo getDbInfo(Connection connection) throws SQLException {
    if (Objects.isNull(spatiaLiteLoader)) {
      // spatialite ist not available in simple client
      return ImmutableDbInfoGpkg.of("unknown", "unknown", SpatialMetadata.GPKG);
    }

    String query =
        "SELECT sqlite_version(),spatialite_version(),CASE CheckSpatialMetaData() WHEN 4 THEN 'GPKG' WHEN 3 THEN 'SPATIALITE' ELSE 'UNSUPPORTED' END;";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(query);
    rs.next();

    return ImmutableDbInfoGpkg.of(
        rs.getString(1), rs.getString(2), SpatialMetadata.valueOf(rs.getString(3)));
  }

  @Override
  public Collator getRowSortingCollator(Optional<String> defaultCollation) {
    return null;
  }

  private EpsgCrs.Force forceAxisOrder(DbInfoGpkg dbInfo) {
    if (dbInfo.getSpatialMetadata() == SpatialMetadata.GPKG) {
      return Force.LON_LAT;
    }

    return Force.NONE;
  }

  @Value.Immutable
  public interface DbInfoGpkg extends DbInfo {
    enum SpatialMetadata {
      GPKG,
      SPATIALITE,
      UNSUPPORTED
    }

    @Value.Parameter
    String getSqliteVersion();

    @Value.Parameter
    String getSpatialiteVersion();

    @Value.Parameter
    SpatialMetadata getSpatialMetadata();
  }
}
