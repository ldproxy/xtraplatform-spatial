/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface SchemaVisitorTopDown<T extends SchemaBase<T>, U> {

  U visit(T schema, List<T> parents, List<U> visitedProperties);

  default Map<String, U> asMap(List<U> visitedProperties, Function<U, String> toKey) {
    return visitedProperties.stream()
        .filter(Objects::nonNull)
        .map(schema -> new SimpleImmutableEntry<>(toKey.apply(schema), schema))
        .collect(
            ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue, (first, second) -> second));
  }
}
