/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

public interface FeatureStoreAttributesContainer {

  String getName();

  List<String> getPath();

  String getSortKey();

  // TODO: implement predicates // NOPMD
  // Optional<FeatureStorePredicate> getPredicate();

  String getInstanceContainerName();

  // TODO: needed for inserts // NOPMD
  // boolean shouldAutoGenerateId();

  List<FeatureStoreAttribute> getAttributes();

  @Value.Derived
  @Value.Auxiliary
  default String getPathString() {
    return "/" + Joiner.on('/').join(getPath());
  }

  @Value.Derived
  @Value.Auxiliary
  default List<List<String>> getAttributePaths() {
    return getAttributes().stream()
        .map(FeatureStoreAttribute::getPath)
        .collect(ImmutableList.toImmutableList());
  }

  @Value.Derived
  @Value.Auxiliary
  default List<String> getSortKeys() {
    return ImmutableList.of(String.format("%s.%s", getName(), getSortKey()));
  }

  @Value.Derived
  @Value.Auxiliary
  default boolean isSpatial() {
    return getAttributes().stream().anyMatch(FeatureStoreAttribute::isSpatial);
  }

  @Value.Derived
  @Value.Auxiliary
  default Optional<FeatureStoreAttribute> getSpatialAttribute() {
    return getAttributes().stream().filter(FeatureStoreAttribute::isSpatial).findFirst();
  }

  @Value.Derived
  @Value.Auxiliary
  default boolean isTemporal(String property) {
    return getAttributes().stream()
        .filter(p -> p.getQueryable().equals(property))
        .anyMatch(FeatureStoreAttribute::isTemporal);
  }

  @Value.Derived
  @Value.Auxiliary
  default Optional<FeatureStoreAttribute> getTemporalAttribute(String property) {
    return getAttributes().stream()
        .filter(FeatureStoreAttribute::isTemporal)
        .filter(p -> p.getQueryable().equals(property))
        .findFirst();
  }

  @Value.Derived
  @Value.Auxiliary
  default boolean isMain() {
    return getAttributes().stream().anyMatch(FeatureStoreAttribute::isId);
  }

  @Value.Derived
  @Value.Auxiliary
  default Optional<FeatureStoreAttribute> getIdAttribute() {
    return getAttributes().stream().filter(FeatureStoreAttribute::isId).findFirst();
  }
}
