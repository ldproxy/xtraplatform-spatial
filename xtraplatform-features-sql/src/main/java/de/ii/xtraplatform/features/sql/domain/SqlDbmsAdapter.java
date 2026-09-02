/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.github.azahnen.dagger.annotations.AutoMultiBind;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.Collator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.immutables.value.Value;

@AutoMultiBind
public interface SqlDbmsAdapter {

  String getId();

  SqlDialect getDialect();

  DataSource createDataSource(String providerId, ConnectionInfoSql connectionInfoSql);

  Optional<String> getInitSql(ConnectionInfoSql connectionInfo);

  List<String> getDefaultSchemas();

  List<String> getSystemSchemas();

  List<String> getSystemTables();

  Map<String, GeoInfo> getGeoInfo(Connection connection, DbInfo dbInfo) throws SQLException;

  /**
   * Returns the geometry columns that have a spatial index which the query generator has to name
   * explicitly to make use of, keyed by {@code table.column} in lower case (SQL identifiers are
   * compared case-insensitively here, as SQLite does for ASCII). The value is the column of the
   * table that the index entries are keyed on, which a query has to join the index on.
   *
   * <p>The default is an empty map, which is correct for every DBMS whose spatial operators consult
   * the spatial index by themselves. It takes the client rather than a connection so that those
   * adapters do not have to be handed one they will not use.
   */
  default Map<String, String> getSpatialIndexes(SqlClientBasic sqlClient) throws SQLException {
    return Map.of();
  }

  DbInfo getDbInfo(Connection connection) throws SQLException;

  Collator getRowSortingCollator(Optional<String> defaultCollation);

  interface DbInfo {}

  @Value.Immutable
  interface GeoInfo {

    String SCHEMA = "schema";
    String TABLE = "table";
    String COLUMN = "column";
    String DIMENSION = "dimension";
    String SRID = "srid";
    String TYPE = "type";

    @Nullable
    @Value.Parameter
    String getSchema();

    @Value.Parameter
    String getTable();

    @Value.Parameter
    String getColumn();

    @Value.Parameter
    String getDimension();

    @Value.Parameter
    String getSrid();

    @Value.Parameter
    String getForce();

    @Value.Parameter
    String getType();
  }
}
