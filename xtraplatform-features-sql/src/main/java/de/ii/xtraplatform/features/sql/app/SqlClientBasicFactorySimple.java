/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import de.ii.xtraplatform.features.sql.domain.SqlClientBasic;
import de.ii.xtraplatform.features.sql.domain.SqlClientBasicFactory;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter.DbInfo;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter.GeoInfo;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapters;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import de.ii.xtraplatform.features.sql.infra.db.SqlConnectorRx;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

public class SqlClientBasicFactorySimple implements SqlClientBasicFactory {
  private final SqlDbmsAdapters dbmsAdapters;

  public SqlClientBasicFactorySimple(SqlDbmsAdapters dbmsAdapters) {
    this.dbmsAdapters = dbmsAdapters;
  }

  @Override
  public SqlClientBasic create(
      String providerType, String providerId, ConnectionInfoSql connectionInfo) {
    DataSource dataSource =
        dbmsAdapters.get(connectionInfo.getDialect()).createDataSource(providerId, connectionInfo);

    return new SqlClientBasicSimple(
        dataSource,
        connectionInfo,
        dbmsAdapters.get(connectionInfo.getDialect()),
        dbmsAdapters.getDialect(connectionInfo.getDialect()));
  }

  @Override
  @SuppressWarnings("PMD.CloseResource")
  public void dispose(SqlClientBasic sqlClient) {
    if (sqlClient instanceof SqlClientBasicSimple) {
      for (Connection connection : ((SqlClientBasicSimple) sqlClient).connections) {
        try {
          connection.close();
        } catch (SQLException e) {
          // ignore
        }
      }
      DataSource dataSource = ((SqlClientBasicSimple) sqlClient).dataSource;
      if (dataSource instanceof Closeable) {
        try {
          ((Closeable) dataSource).close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }

  private static final class SqlClientBasicSimple implements SqlClientBasic {
    private final DataSource dataSource;
    private final ConnectionInfoSql connectionInfo;
    private final SqlDbmsAdapter dbmsAdapter;
    private final SqlDialect dialect;
    private final List<Connection> connections;

    private SqlClientBasicSimple(
        DataSource dataSource,
        ConnectionInfoSql connectionInfo,
        SqlDbmsAdapter dbmsAdapter,
        SqlDialect dialect) {
      this.dataSource = dataSource;
      this.connectionInfo = connectionInfo;
      this.dbmsAdapter = dbmsAdapter;
      this.dialect = dialect;
      this.connections = new ArrayList<>();
    }

    @Override
    public Connection getConnection() {
      Connection connection;
      try {
        connection =
            dataSource.getConnection(
                connectionInfo.getUser().orElse(""), SqlConnectorRx.getPassword(connectionInfo));
      } catch (SQLException e) {
        throw new IllegalStateException(e);
      }

      this.connections.add(connection);

      return connection;
    }

    @Override
    public SqlDialect getSqlDialect() {
      return dialect;
    }

    @Override
    public SqlDbmsAdapter getDbmsAdapter() {
      return dbmsAdapter;
    }

    @Override
    public DbInfo getDbInfo() throws SQLException {
      Connection connection = connections.isEmpty() ? getConnection() : connections.get(0);

      return dbmsAdapter.getDbInfo(connection);
    }

    @Override
    public Map<String, GeoInfo> getGeoInfo() throws SQLException {
      Connection connection = connections.isEmpty() ? getConnection() : connections.get(0);

      return dbmsAdapter.getGeoInfo(connection, getDbInfo());
    }
  }
}
