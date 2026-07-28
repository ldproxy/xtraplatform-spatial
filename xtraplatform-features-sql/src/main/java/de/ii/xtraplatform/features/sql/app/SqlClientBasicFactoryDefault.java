/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.domain.ConnectorFactory;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import de.ii.xtraplatform.features.sql.domain.SqlClientBasic;
import de.ii.xtraplatform.features.sql.domain.SqlClientBasicFactory;
import de.ii.xtraplatform.features.sql.domain.SqlConnector;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapters;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import java.sql.Connection;

public class SqlClientBasicFactoryDefault implements SqlClientBasicFactory {
  private final ConnectorFactory connectorFactory;
  private final SqlDbmsAdapters dbmsAdapters;

  public SqlClientBasicFactoryDefault(
      ConnectorFactory connectorFactory, SqlDbmsAdapters dbmsAdapters) {
    this.connectorFactory = connectorFactory;
    this.dbmsAdapters = dbmsAdapters;
  }

  @Override
  public SqlClientBasic create(
      String providerType, String providerId, ConnectionInfoSql connectionInfo) {
    SqlConnector connector =
        (SqlConnector) connectorFactory.createConnector(providerType, providerId, connectionInfo);

    if (!connector.isConnected()) {
      connectorFactory.disposeConnector(connector);

      throw connector
          .getConnectionError()
          .map(
              throwable ->
                  throwable instanceof RuntimeException
                      ? (RuntimeException) throwable
                      : new IllegalStateException(throwable))
          .orElse(new IllegalStateException("unknown reason"));
    }

    return new SqlClientBasicDefault(
        connector,
        dbmsAdapters.get(connectionInfo.getDialect()),
        dbmsAdapters.getDialect(connectionInfo.getDialect()));
  }

  @Override
  public void dispose(SqlClientBasic sqlClient) {
    if (sqlClient instanceof SqlClientBasicDefault) {
      connectorFactory.disposeConnector(((SqlClientBasicDefault) sqlClient).connector);
    }
  }

  private static final class SqlClientBasicDefault implements SqlClientBasic {
    private final SqlConnector connector;
    private final SqlDbmsAdapter dbmsAdapter;
    private final SqlDialect dialect;

    private SqlClientBasicDefault(
        SqlConnector connector, SqlDbmsAdapter dbmsAdapter, SqlDialect dialect) {
      this.connector = connector;
      this.dbmsAdapter = dbmsAdapter;
      this.dialect = dialect;
    }

    @Override
    public Connection getConnection() {
      return connector.getSqlClient().getConnection();
    }

    @Override
    public SqlDialect getSqlDialect() {
      return dialect;
    }

    @Override
    public SqlDbmsAdapter getDbmsAdapter() {
      return dbmsAdapter;
    }
  }
}
