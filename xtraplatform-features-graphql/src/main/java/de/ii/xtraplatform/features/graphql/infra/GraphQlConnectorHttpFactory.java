/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.graphql.infra;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableSet;
import dagger.assisted.AssistedFactory;
import de.ii.xtraplatform.features.domain.ConnectionInfo;
import de.ii.xtraplatform.features.domain.ConnectorFactory2;
import de.ii.xtraplatform.features.domain.FeatureProviderConnector;
import de.ii.xtraplatform.features.graphql.app.FeatureProviderGraphQl;
import de.ii.xtraplatform.features.graphql.domain.ConnectionInfoGraphQlHttp;
import de.ii.xtraplatform.features.graphql.domain.GraphQlConnector;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@AutoBind
public class GraphQlConnectorHttpFactory
    implements ConnectorFactory2<byte[], String, FeatureProviderConnector.QueryOptions> {

  private final FactoryAssisted factoryAssisted;
  private final Map<String, GraphQlConnector> instances;

  @Inject
  public GraphQlConnectorHttpFactory(FactoryAssisted factoryAssisted) {
    this.factoryAssisted = factoryAssisted;
    this.instances = new LinkedHashMap<>();
  }

  @Override
  public String type() {
    return FeatureProviderGraphQl.PROVIDER_TYPE;
  }

  @Override
  public Optional<String> subType() {
    return Optional.of(GraphQlConnectorHttp.CONNECTOR_TYPE);
  }

  @Override
  public Optional<FeatureProviderConnector<byte[], String, FeatureProviderConnector.QueryOptions>>
      instance(String id) {
    return Optional.ofNullable(instances.get(id));
  }

  @Override
  public Set<FeatureProviderConnector<byte[], String, FeatureProviderConnector.QueryOptions>>
      instances() {
    return ImmutableSet.copyOf(instances.values());
  }

  @Override
  public FeatureProviderConnector<byte[], String, FeatureProviderConnector.QueryOptions>
      createInstance(String providerId, ConnectionInfo connectionInfo) {
    GraphQlConnector wfsConnector =
        factoryAssisted.create(providerId, (ConnectionInfoGraphQlHttp) connectionInfo);
    wfsConnector.start();
    instances.put(wfsConnector.getProviderId(), wfsConnector);

    return wfsConnector;
  }

  @Override
  public boolean deleteInstance(String id) {
    if (instances.containsKey(id)) {
      instance(id).ifPresent(FeatureProviderConnector::stop);
      instances.remove(id);
      return true;
    }
    return false;
  }

  @AssistedFactory
  @FunctionalInterface
  public interface FactoryAssisted {
    GraphQlConnectorHttp create(String providerId, ConnectionInfoGraphQlHttp connectionInfo);
  }
}
