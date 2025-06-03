/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.app.FeatureDataSql;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Transformer;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public interface SqlClient extends SqlClientBasic {

  CompletableFuture<Collection<SqlRow>> run(String query, SqlQueryOptions options);

  Reactive.Source<SqlRow> getSourceStream(String query, SqlQueryOptions options);

  Reactive.Source<String> getMutationSource(
      List<Supplier<String>> statements,
      List<Consumer<String>> idConsumers,
      Object executionContext);

  Transformer<FeatureDataSql, String> getMutationFlow(
      Function<FeatureDataSql, List<Supplier<Tuple<String, Consumer<String>>>>> mutations,
      Object executionContext,
      Optional<String> id);

  List<String> getNotifications(Connection connection);
}
