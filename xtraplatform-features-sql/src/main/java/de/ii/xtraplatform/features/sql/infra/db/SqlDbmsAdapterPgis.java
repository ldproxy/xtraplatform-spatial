/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.crs.domain.EpsgCrs.Force;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import de.ii.xtraplatform.features.sql.domain.ImmutableGeoInfo;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsPgis;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Collator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Locale.Builder;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import org.davidmoten.rxjava3.jdbc.pool.DatabaseType;
import org.immutables.value.Value;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class SqlDbmsAdapterPgis implements SqlDbmsAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlDbmsAdapterPgis.class);

  private final String applicationName;
  private final SqlDialect dialect;

  @Inject
  public SqlDbmsAdapterPgis(AppContext appContext) {
    this.applicationName =
        String.format("%s %s - %%s", appContext.getName(), appContext.getVersion());
    this.dialect = new SqlDialectPgis();
  }

  @Override
  public String getId() {
    return SqlDbmsPgis.ID;
  }

  @Override
  public SqlDialect getDialect() {
    return dialect;
  }

  @Override
  public DataSource createDataSource(String providerId, ConnectionInfoSql connectionInfo) {
    PGSimpleDataSource ds = new PGSimpleDataSource();

    ds.setAssumeMinServerVersion("9.6");
    ds.setApplicationName(String.format(applicationName, providerId));

    ds.setServerName(
        connectionInfo
            .getHost()
            .orElseThrow(
                () ->
                    new IllegalArgumentException("No 'host' given, required for PGIS connection")));

    ds.setDatabaseName(
        Optional.ofNullable(Strings.emptyToNull(connectionInfo.getDatabase()))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "No 'database' given, required for PGIS connection")));

    connectionInfo
        .getDriverOptions()
        .forEach(
            (key, value) -> {
              switch (key) {
                case "gssEncMode":
                  ds.setGssEncMode(String.valueOf(value));
                  break;
                case "ssl":
                  ds.setSsl(Objects.equals(value, "true"));
                  break;
                case "sslmode":
                  ds.setSslMode(String.valueOf(value));
                  break;
                case "sslcert":
                  ds.setSslCert(String.valueOf(value));
                  break;
                case "sslkey":
                  ds.setSslKey(String.valueOf(value));
                  break;
                case "sslrootcert":
                  ds.setSslRootCert(String.valueOf(value));
                  break;
                case "sslpassword":
                  ds.setSslPassword(String.valueOf(value));
                  break;
              }
            });

    return ds;
  }

  @Override
  public Optional<String> getInitSql(ConnectionInfoSql connectionInfo) {
    String initSql = "SET datestyle TO ISO, MDY; SET client_encoding TO UTF8; SET timezone TO UTC;";
    if (!connectionInfo.getSchemas().isEmpty()) {
      String schemas =
          connectionInfo.getSchemas().stream()
              .map(
                  schema -> {
                    if (!Objects.equals(schema, schema.toLowerCase())) {
                      return String.format("\"%s\"", schema);
                    }
                    return schema;
                  })
              .collect(Collectors.joining(","));

      return Optional.of(String.format("SET search_path TO %s,public; %s", schemas, initSql));
    }
    return Optional.of(initSql);
  }

  @Override
  public DatabaseType getRxType() {
    return DatabaseType.POSTGRES;
  }

  @Override
  public List<String> getDefaultSchemas() {
    return List.of("public");
  }

  @Override
  public List<String> getSystemSchemas() {
    return ImmutableList.of("information_schema", "pg_catalog", "tiger", "tiger_data", "topology");
  }

  @Override
  public List<String> getSystemTables() {
    return ImmutableList.of(
        "spatial_ref_sys",
        "geography_columns",
        "geometry_columns",
        "raster_columns",
        "raster_overviews");
  }

  @Override
  public Map<String, GeoInfo> getGeoInfo(Connection connection, DbInfo dbInfo) throws SQLException {
    if (!(dbInfo instanceof DbInfoPgis)) {
      throw new SQLException("Not a valid spatial PostgreSQL database.");
    }
    String query =
        String.format(
            "SELECT f_table_schema AS \"%s\", f_table_name AS \"%s\", f_geometry_column AS \"%s\", coord_dimension AS \"%s\", srid AS \"%s\", type AS \"%s\" FROM geometry_columns;",
            GeoInfo.SCHEMA,
            GeoInfo.TABLE,
            GeoInfo.COLUMN,
            GeoInfo.DIMENSION,
            GeoInfo.SRID,
            GeoInfo.TYPE);

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(query);
    Map<String, GeoInfo> result = new LinkedHashMap<>();

    while (rs.next()) {
      result.put(
          rs.getString(GeoInfo.TABLE),
          ImmutableGeoInfo.of(
              rs.getString(GeoInfo.SCHEMA),
              rs.getString(GeoInfo.TABLE),
              rs.getString(GeoInfo.COLUMN),
              rs.getString(GeoInfo.DIMENSION),
              rs.getString(GeoInfo.SRID),
              Force.NONE.name(),
              rs.getString(GeoInfo.TYPE)));
    }

    return result;
  }

  @Override
  public DbInfo getDbInfo(Connection connection) throws SQLException {
    String query = "SELECT version(), PostGIS_Lib_Version();";

    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(query);
    rs.next();

    return ImmutableDbInfoPgis.of(rs.getString(1), rs.getString(2));
  }

  @Override
  public Collator getRowSortingCollator(Optional<String> defaultCollation) {
    String languageTag = defaultCollation.orElse(Locale.US.toLanguageTag());

    if (Objects.equals(languageTag, "C")) {
      return null;
    }

    Locale locale;
    try {
      locale = new Builder().setLanguageTag(languageTag).build();
    } catch (Exception e) {
      locale = Locale.US;

      LOGGER.warn(
          "Invalid default collation '{}', falling back to '{}' for sorting: {}",
          languageTag,
          Locale.US.toLanguageTag(),
          e.getMessage());
    }

    return Collator.getInstance(locale);
  }

  @Value.Immutable
  interface DbInfoPgis extends DbInfo {

    @Value.Parameter
    String getPostgresVersion();

    @Value.Parameter
    String getPostGisVersion();
  }
}
