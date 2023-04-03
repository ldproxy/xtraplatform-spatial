/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.features.domain.SourcePath;
import de.ii.xtraplatform.features.domain.Tuple;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable
public interface SqlPath extends SourcePath {

  String getName();

  List<SqlPath> getParentTables();

  Optional<Tuple<String, String>> getJoin();

  Optional<String> getConnector();

  List<String> getColumns();

  String getSortKey();

  String getPrimaryKey();

  Optional<Cql2Expression> getFilter();

  Optional<String> getFilterString();

  // TODO: not needed any more? should be based on primary key detection
  boolean getJunction();

  @Value.Derived
  default boolean isRoot() {
    return getColumns().isEmpty() && !getJoin().isPresent();
  }

  @Value.Derived
  default boolean isBranch() {
    return !isRoot() && !isLeaf();
  }

  @Value.Derived
  default boolean isConnected() {
    return !isRoot() && getConnector().isPresent();
  }

  @Value.Derived
  default boolean isLeaf() {
    return !getColumns().isEmpty();
  }

  // TODO: not needed any more? should be based on primary key detection
  @Value.Derived
  default boolean isJunction() {
    return getJunction();
  }

  @Value.Derived
  default String asPath() {
    return isBranch()
        ? String.format(
            "[%s=%s]%s%s",
            getJoin().get().first(),
            getJoin().get().second(),
            getName(),
            getFilterString().map(filterString -> "{filter=" + filterString + "}").orElse(""))
        : isConnected()
            ? String.format("[%s]%s", getConnector().get(), getName())
            : getName()
                + getFilterString().map(filterString -> "{filter=" + filterString + "}").orElse("");
  }

  @Value.Derived
  default List<String> getParentPath() {
    return getParentTables().stream().map(SqlPath::asPath).collect(Collectors.toList());
  }

  @Override
  @Value.Derived
  default List<String> getFullPath() {
    return Stream.concat(getParentPath().stream(), Stream.of(asPath()))
        .collect(Collectors.toList());
  }
}
