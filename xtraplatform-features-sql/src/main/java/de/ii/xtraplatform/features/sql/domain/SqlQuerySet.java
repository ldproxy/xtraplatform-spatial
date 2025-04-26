/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.commons.lang3.function.TriFunction;
import org.immutables.value.Value;

@Value.Immutable
public interface SqlQuerySet {

  BiFunction<Long, Long, Optional<String>> getMetaQuery();

  TriFunction<SqlRowMeta, Long, Long, Stream<String>> getValueQueries();

  List<SqlQuerySchema> getTableSchemas();

  SqlQueryOptions getOptions();
}
