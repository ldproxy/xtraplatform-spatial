/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Consumer;
import us.fatehi.utility.datasource.DatabaseConnectionSource;
import us.fatehi.utility.datasource.PooledConnectionUtility;

class SingleDatabaseConnectionSource implements DatabaseConnectionSource {
  private final Connection connection;
  private Consumer<Connection> connectionInitializer;

  SingleDatabaseConnectionSource(Connection connection) {
    this.connection = connection;
    this.connectionInitializer = c -> {};
  }

  @Override
  public Connection get() {
    this.connectionInitializer.accept(this.connection);
    return PooledConnectionUtility.newPooledConnection(this.connection, this);
  }

  @Override
  public boolean releaseConnection(Connection connection) {
    return true;
  }

  @Override
  public void setFirstConnectionInitializer(Consumer<Connection> connectionInitializer) {
    if (connectionInitializer != null) {
      this.connectionInitializer = connectionInitializer.andThen(this.connectionInitializer);
    }
  }

  @Override
  public void close() throws SQLException {
    this.connection.close();
  }
}
