/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.geoparquet.app;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class DuckdbDataSource implements javax.sql.DataSource {

  @Override
  public Connection getConnection() throws SQLException {
    try {
      Class.forName("org.duckdb.DuckDBDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return DriverManager.getConnection("jdbc:duckdb:");
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    // Username and password are irrelevant for accessing Parquet files with DuckDB
    return getConnection();
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter printWriter) throws SQLException {}

  @Override
  public void setLoginTimeout(int i) throws SQLException {}

  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return false;
  }
}
