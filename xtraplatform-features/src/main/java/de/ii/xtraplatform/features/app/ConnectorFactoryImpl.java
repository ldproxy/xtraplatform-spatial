/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import dagger.Lazy;
import de.ii.xtraplatform.features.domain.ConnectionInfo;
import de.ii.xtraplatform.features.domain.ConnectorFactory;
import de.ii.xtraplatform.features.domain.ConnectorFactory2;
import de.ii.xtraplatform.features.domain.FeatureProviderConnector;
import de.ii.xtraplatform.features.domain.Tuple;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zahnen
 */
@Singleton
@AutoBind
public class ConnectorFactoryImpl implements ConnectorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorFactoryImpl.class);

  private final Lazy<Set<ConnectorFactory2<?, ?, ?>>> connectorFactories;
  private final Map<String, Set<Runnable>> disposeListeners;
  private final ReentrantLock lock = new ReentrantLock();

  @Inject
  public ConnectorFactoryImpl(Lazy<Set<ConnectorFactory2<?, ?, ?>>> connectorFactories) {
    this.connectorFactories = connectorFactories;
    this.disposeListeners = new HashMap<>();
  }

  @Override
  public FeatureProviderConnector<?, ?, ?> createConnector(
      String providerType, String providerId, ConnectionInfo connectionInfo) {
    lock.lock();
    try {
      final String connectorType = connectionInfo.getConnectorType();

      if (getFactory(providerType, connectorType).isEmpty()) {
        throw new IllegalStateException(
            String.format(
                "Connector with type %s for provider type %s is not supported.",
                connectorType, providerType));
      }

      ConnectorFactory2<?, ?, ?> connectorFactory2 = getFactory(providerType, connectorType).get();

      if (connectionInfo.isShared()) {
        Optional<FeatureProviderConnector<?, ?, ?>> shared =
            findSharedConnector(connectorFactory2, connectionInfo);

        if (shared.isPresent()) {
          return shared.get();
        }
      }

      return createNewConnector(
          connectorFactory2, providerId, connectorType, providerType, connectionInfo);
    } finally {
      lock.unlock();
    }
  }

  private Optional<FeatureProviderConnector<?, ?, ?>> findSharedConnector(
      ConnectorFactory2<?, ?, ?> connectorFactory2, ConnectionInfo connectionInfo) {
    Optional<? extends FeatureProviderConnector<?, ?, ?>> match =
        connectorFactory2.instances().stream()
            .filter(connector -> connector.canBeSharedWith(connectionInfo, false).first())
            .findFirst();

    if (match.isEmpty()) {
      return Optional.empty();
    }

    Tuple<Boolean, String> fullMatch = match.get().canBeSharedWith(connectionInfo, true);

    if (!fullMatch.first()) {
      throw new IllegalStateException(
          String.format(
              "Connection pool cannot be shared with provider %s: %s",
              match.get().getProviderId(), fullMatch.second()));
    }

    LOGGER.debug("Joining shared pool.");
    match
        .get()
        .getRefCounter()
        .ifPresent(refs -> LOGGER.debug("Shared pool consumers: {}", refs.incrementAndGet()));

    return Optional.of(match.get());
  }

  @SuppressWarnings("PMD.AvoidCatchingGenericException")
  private FeatureProviderConnector<?, ?, ?> createNewConnector(
      ConnectorFactory2<?, ?, ?> connectorFactory2,
      String providerId,
      String connectorType,
      String providerType,
      ConnectionInfo connectionInfo) {
    try {
      LOGGER.debug("Creating new pool.");
      FeatureProviderConnector<?, ?, ?> connector =
          connectorFactory2.createInstance(providerId, connectionInfo);

      if (connectionInfo.isShared()) {
        connector
            .getRefCounter()
            .ifPresent(refs -> LOGGER.debug("Shared pool consumers: {}", refs.incrementAndGet()));
      }

      return connector;

    } catch (Exception e) {
      throw new IllegalStateException(
          String.format(
              "Connector with type %s for provider type %s could not be created.",
              connectorType, providerType),
          e);
    }
  }

  @Override
  public void disposeConnector(FeatureProviderConnector<?, ?, ?> connector) {
    lock.lock();
    try {
      int refs = 0;
      if (connector.getRefCounter().isPresent()) {
        LOGGER.debug("Leaving shared pool.");
        refs = connector.getRefCounter().get().decrementAndGet();
        LOGGER.debug("Shared pool consumers: {}", refs);
      }

      if (refs == 0) {
        boolean deleted =
            getFactory(connector.getType()).get().deleteInstance(connector.getProviderId());
        if (deleted && LOGGER.isDebugEnabled()) {
          LOGGER.debug("Deleted unused pool.");
        }
      }

      if (disposeListeners.containsKey(connector.getProviderId())) {
        disposeListeners.get(connector.getProviderId()).forEach(Runnable::run);
        disposeListeners.get(connector.getProviderId()).clear();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onDispose(FeatureProviderConnector<?, ?, ?> connector, Runnable runnable) {
    lock.lock();
    try {
      if (!disposeListeners.containsKey(connector.getProviderId())) {
        disposeListeners.put(connector.getProviderId(), new HashSet<>());
      }
      disposeListeners.get(connector.getProviderId()).add(runnable);
    } finally {
      lock.unlock();
    }
  }

  private Optional<ConnectorFactory2<?, ?, ?>> getFactory(String type, String subType) {
    return connectorFactories.get().stream()
        .filter(
            connectorFactory2 ->
                Objects.equals(type, connectorFactory2.type())
                    && connectorFactory2
                        .subType()
                        .filter(s -> Objects.equals(subType, s))
                        .isPresent())
        .findFirst();
  }

  private Optional<ConnectorFactory2<?, ?, ?>> getFactory(String fullType) {
    return connectorFactories.get().stream()
        .filter(connectorFactory2 -> Objects.equals(fullType, connectorFactory2.fullType()))
        .findFirst();
  }
}
