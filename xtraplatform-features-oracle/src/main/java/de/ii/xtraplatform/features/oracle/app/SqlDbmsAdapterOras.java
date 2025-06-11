/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.oracle.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.base.Strings;
import de.ii.xtraplatform.base.domain.AppContext;
import de.ii.xtraplatform.blobs.domain.ResourceStore;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.Collator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import org.davidmoten.rxjava3.jdbc.pool.DatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class SqlDbmsAdapterOras implements SqlDbmsAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlDbmsAdapterOras.class);

  static final String ID = "ORAS";
  private static final String JDBC_MODULE = "com.oracle.database.jdbc";
  private static final String I18N_MODULE = "com.oracle.database.nls.orai18n";
  private static final String JDBC_DS_CLASS = "oracle.jdbc.datasource.impl.OracleDataSource";

  private final String applicationName;
  private final SqlDialect dialect;
  private final ResourceStore driverStore;

  @Inject
  public SqlDbmsAdapterOras(AppContext appContext, ResourceStore resourceStore) {
    this.applicationName =
        String.format("%s %s - %%s", appContext.getName(), appContext.getVersion());
    this.dialect = new SqlDialectOras();
    this.driverStore = resourceStore.with("oracle");
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
    String host =
        connectionInfo
            .getHost()
            .orElseThrow(
                () ->
                    new IllegalArgumentException("No 'host' given, required for ORAS connection"));
    String database =
        Optional.ofNullable(Strings.emptyToNull(connectionInfo.getDatabase()))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "No 'database' given, required for ORAS connection"));

    Class<?> oracleDataSource = loadDriver();

    try {
      DataSource ds =
          oracleDataSource.asSubclass(DataSource.class).getDeclaredConstructor().newInstance();

      oracleDataSource
          .getDeclaredMethod("setDescription", String.class)
          .invoke(ds, String.format(applicationName, providerId));
      oracleDataSource.getDeclaredMethod("setDriverType", String.class).invoke(ds, "thin");

      if (host.contains(":")) {
        oracleDataSource
            .getDeclaredMethod("setServerName", String.class)
            .invoke(ds, host.substring(0, host.indexOf(":")));
        oracleDataSource
            .getDeclaredMethod("setPortNumber", int.class)
            .invoke(ds, Integer.parseInt(host.substring(host.lastIndexOf(":") + 1)));
      } else {
        oracleDataSource.getDeclaredMethod("setServerName", String.class).invoke(ds, host);
      }

      oracleDataSource.getDeclaredMethod("setServiceName", String.class).invoke(ds, database);

      return ds;
    } catch (Throwable e) {
      throw new IllegalStateException("Could not create Oracle data source", e);
    }
  }

  private Class<?> loadDriver() {
    try {
      Path driverPath = driverStore.asLocalPath(Path.of(""), false).get();
      ModuleFinder finder = ModuleFinder.of(driverPath);
      ModuleLayer parent = ModuleLayer.boot();
      Configuration cf =
          parent.configuration().resolve(finder, ModuleFinder.of(), Set.of(JDBC_MODULE));
      ModuleLayer layer = parent.defineModulesWithOneLoader(cf, ClassLoader.getSystemClassLoader());

      return layer.findLoader(JDBC_MODULE).loadClass(JDBC_DS_CLASS);
    } catch (Throwable e) {
      throw new IllegalStateException("Could not load Oracle driver", e);
    }
  }

  @Override
  public Optional<String> getInitSql(ConnectionInfoSql connectionInfo) {
    return Optional.empty();
  }

  @Override
  public DatabaseType getRxType() {
    return DatabaseType.ORACLE;
  }

  @Override
  public List<String> getDefaultSchemas() {
    return List.of("public");
  }

  @Override
  public List<String> getSystemSchemas() {
    return List.of();
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
