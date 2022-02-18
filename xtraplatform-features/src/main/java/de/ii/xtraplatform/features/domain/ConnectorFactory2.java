/**
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.github.azahnen.dagger.annotations.AutoMultiBind;
import de.ii.xtraplatform.store.domain.KeyPathAlias;
import de.ii.xtraplatform.store.domain.entities.EntityData;
import de.ii.xtraplatform.store.domain.entities.EntityDataBuilder;
import de.ii.xtraplatform.store.domain.entities.PersistentEntity;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@AutoMultiBind
public interface ConnectorFactory2<T,U,V extends FeatureProviderConnector.QueryOptions> {

  String type();

  default Optional<String> subType() {
    return Optional.empty();
  }

  default String fullType() {
    if (subType().isPresent()) {
      return String.format("%s/%s", type(), subType().get());
    }

    return type();
  }

  Optional<FeatureProviderConnector<T,U,V>> instance(String id);

  Set<FeatureProviderConnector<T,U,V>> instances();

  FeatureProviderConnector<T,U,V> createInstance(FeatureProviderDataV2 data);

  void deleteInstance(String id);
}
