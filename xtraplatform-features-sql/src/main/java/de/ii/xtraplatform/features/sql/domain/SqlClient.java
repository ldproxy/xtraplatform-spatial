/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import de.ii.xtraplatform.streams.domain.Reactive;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SqlClient extends SqlClientBasic {

  CompletableFuture<Collection<SqlRow>> run(String query, SqlQueryOptions options);

  Reactive.Source<SqlRow> getSourceStream(String query, SqlQueryOptions options);

  List<String> getNotifications(Connection connection);

  /**
   * Releases a connection from {@link #getConnection()} that must not be reused, because its
   * session state may have been changed, for example the search path. A pooled implementation
   * removes the connection from the pool, so that the pool opens a new one with the configured
   * initialization. The default implementation closes the connection.
   */
  default void discardConnection(Connection connection) throws SQLException {
    connection.close();
  }

  /**
   * Opens a synchronous, single-connection session for multi-statement transactions. The default
   * implementation throws {@link UnsupportedOperationException}.
   */
  default SqlSession openSession() {
    throw new UnsupportedOperationException(
        "Synchronous SQL sessions are not supported by this SqlClient implementation");
  }
}
