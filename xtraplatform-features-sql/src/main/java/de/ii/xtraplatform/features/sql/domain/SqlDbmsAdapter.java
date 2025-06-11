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
import org.davidmoten.rxjava3.jdbc.pool.DatabaseType;
import org.immutables.value.Value;

@AutoMultiBind
public interface SqlDbmsAdapter {

  String getId();

  SqlDialect getDialect();

  DataSource createDataSource(String providerId, ConnectionInfoSql connectionInfoSql);

  Optional<String> getInitSql(ConnectionInfoSql connectionInfo);

  List<String> getDefaultSchemas();

  DatabaseType getRxType();

  List<String> getSystemSchemas();

  List<String> getSystemTables();

  Map<String, GeoInfo> getGeoInfo(Connection connection, DbInfo dbInfo) throws SQLException;

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
