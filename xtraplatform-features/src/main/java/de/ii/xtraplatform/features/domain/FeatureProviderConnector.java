/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import de.ii.xtraplatform.base.domain.JacksonProvider;
import de.ii.xtraplatform.base.domain.resiliency.Volatile2;
import de.ii.xtraplatform.streams.domain.Reactive;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zahnen
 * @param <T> query result type
 * @param <U> query type
 * @param <V> options type
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.CUSTOM,
    include = JsonTypeInfo.As.PROPERTY,
    property = FeatureProviderConnector.CONNECTOR_TYPE_KEY,
    visible = true)
@JsonTypeIdResolver(JacksonProvider.DynamicTypeIdResolver.class)
public interface FeatureProviderConnector<T, U, V extends FeatureProviderConnector.QueryOptions>
    extends Volatile2 {

  String CONNECTOR_TYPE_KEY = "connectorType";

  String VOLATILE_KEY = "connector";

  interface QueryOptions {}

  String getType();

  String getProviderId();

  void start();

  void stop();

  boolean isConnected();

  Optional<Throwable> getConnectionError();

  Reactive.Source<T> getSourceStream(U query);

  Reactive.Source<T> getSourceStream(U query, V options);

  boolean isSameDataset(ConnectionInfo connectionInfo);

  String getDatasetIdentifier();

  @SuppressWarnings("PMD.LinguisticNaming")
  default Tuple<Boolean, String> canBeSharedWith(
      ConnectionInfo connectionInfo, boolean checkAllParameters) {
    return Tuple.of(false, null);
  }

  default Optional<AtomicInteger> getRefCounter() {
    return Optional.empty();
  }
}
