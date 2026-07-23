/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public final class SqlHelper {

  private static final Properties READ_ONLY_PROPERTIES = getReadOnly();

  private SqlHelper() {
    // utility class
  }

  public static Properties getReadOnly() {
    Properties properties = new Properties();
    properties.put("open_mode", "1");
    return properties;
  }

  public static Connection getConnection(Path mbtilesFile, boolean readOnly) {
    try {
      Class.forName("org.sqlite.JDBC");
      return readOnly
          ? DriverManager.getConnection("jdbc:sqlite:" + mbtilesFile, READ_ONLY_PROPERTIES)
          : DriverManager.getConnection("jdbc:sqlite:" + mbtilesFile);
    } catch (SQLException | ClassNotFoundException e) {
      throw new IllegalStateException(
          "Connection to Mbtiles database could not be established.", e);
    }
  }

  public static void execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  public static void addMetadata(Connection connection, String name, Object value) {
    try (PreparedStatement statement =
        connection.prepareStatement("INSERT INTO metadata (name,value) VALUES(?,?)")) {
      statement.setString(1, name);
      if (value instanceof String) {
        statement.setString(2, (String) value);
      } else if (value instanceof Integer) {
        statement.setInt(2, (int) value);
      } else if (value instanceof Long) {
        statement.setLong(2, (long) value);
      } else if (value instanceof Float) {
        statement.setFloat(2, (float) value);
      } else if (value instanceof Double) {
        statement.setDouble(2, (double) value);
      } else {
        statement.setString(2, value.toString());
      }
      statement.execute();
    } catch (SQLException e) {
      throw new IllegalStateException(
          String.format("Could not add metadata: %s=%s", name, value.toString()), e);
    }
  }
}
