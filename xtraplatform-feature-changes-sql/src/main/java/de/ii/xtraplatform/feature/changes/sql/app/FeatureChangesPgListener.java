/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.changes.sql.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.feature.changes.sql.domain.FeatureChangesPgConfiguration;
import de.ii.xtraplatform.features.domain.ExtensionConfiguration;
import de.ii.xtraplatform.features.domain.FeatureChange;
import de.ii.xtraplatform.features.domain.FeatureChanges;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureProviderConnector;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.FeatureProviderEntity;
import de.ii.xtraplatform.features.domain.FeatureQueriesExtension;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.Query;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData;
import de.ii.xtraplatform.features.sql.domain.SqlClient;
import de.ii.xtraplatform.features.sql.domain.SqlConnector;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsPgis;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @title Change Listener
 * @langEn Listens for incremental changes in a PostgreSQL database and triggers feature change
 *     actions.
 * @langDe Registriert inkrementelle Änderungen in einer PostgreSQL Datenbank und löst
 *     Feature-Change-Aktionen aus.
 * @scopeEn This extension can be useful when the database is changed incrementally by other
 *     applications. When a change happens that is relevant for a feature type, registered actions
 *     will be triggered, for example to update the spatial extent or the feature count of an API.
 *     <p>You do not need this extension when changes are only made via
 *     [CRUD](../../../services/building-blocks/crud.md) or when a new iteration of the database is
 *     used after external updates.
 * @scopeDe Diese Erweiterung ist hilfreich, falls inkrementelle Änderungen an der Datenbank durch
 *     andere Anwendungen vorgenommen werden. Wenn eine für eine Objektart relevante Änderung
 *     stattfindet, werden registrierte Aktionen ausgelöst, z.B. die Aktualisierung des
 *     Spatial-Extents oder Feature-Counts einer API.
 *     <p>Diese Erweiterung wird nicht benötigt, wenn Änderungen nur via
 *     [CRUD](../../../services/building-blocks/crud.md) vorgenommen werden oder wenn nach externen
 *     Updates eine neue Iteration der Datenbank verwendet wird.
 * @limitationsEn Only feature types from an SQL feature provider with `dialect` `PGIS` and without
 *     root concatenation are supported.
 * @limitationsDe Es werden nur Objektarten von einem SQL-Feature-Provider mit `dialect` `PGIS` und
 *     ohne Objekt-Konkatenation unterstützt.
 * @ref:propertyTable {@link
 *     de.ii.xtraplatform.feature.changes.sql.domain.ImmutableFeatureChangesPgConfiguration}
 * @ref:example {@link de.ii.xtraplatform.feature.changes.sql.domain.FeatureChangesPgConfiguration}
 */
@Singleton
@AutoBind
public class FeatureChangesPgListener implements FeatureQueriesExtension {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureChangesPgListener.class);

  private final ScheduledExecutorService executorService;
  private final Map<String, Map<String, Subscription>> subscriptions;

  @Inject
  public FeatureChangesPgListener() {
    this.executorService =
        MoreExecutors.getExitingScheduledExecutorService(
            (ScheduledThreadPoolExecutor)
                Executors.newScheduledThreadPool(
                    1, new ThreadFactoryBuilder().setNameFormat("feature.changes-%d").build()));
    this.subscriptions = new ConcurrentHashMap<>();
  }

  @Override
  public boolean isSupported(
      FeatureProviderConnector<?, ?, ?> connector, FeatureProviderDataV2 data) {
    if (!(connector instanceof SqlConnector)
        || !Objects.equals(((SqlConnector) connector).getDialect(), SqlDbmsPgis.ID)
        || !(data instanceof FeatureProviderSqlData)
        || getConfiguration(data.getExtensions()).isEmpty()) {
      return false;
    }
    if (!((FeatureProviderSqlData) data).getDatasetChanges().isModeTrigger()) {
      LOGGER.warn(
          "Feature provider with id '{}' does not support change listeners: datasetChanges.mode is not 'TRIGGER'",
          data.getId());
      return false;
    }

    return true;
  }

  @Override
  public void on(
      LIFECYCLE_HOOK hook,
      FeatureProviderEntity provider,
      FeatureProviderConnector<?, ?, ?> connector) {
    Optional<FeatureChangesPgConfiguration> configuration =
        getConfiguration(provider.getData().getExtensions());

    if (configuration.isPresent()) {
      SqlClient sqlClient = ((SqlConnector) connector).getSqlClient();

      switch (hook) {
        case STARTED:
          subscribe(
              provider, configuration.get(), sqlClient::getConnection, sqlClient::getNotifications);
          break;
      }
    }
  }

  @Override
  public void on(
      QUERY_HOOK hook,
      FeatureProviderDataV2 data,
      FeatureProviderConnector<?, ?, ?> connector,
      Query query,
      BiConsumer<String, String> aliasResolver) {}

  private Optional<FeatureChangesPgConfiguration> getConfiguration(
      List<ExtensionConfiguration> extensions) {
    return extensions.stream()
        .filter(
            extension ->
                extension.isEnabled() && extension instanceof FeatureChangesPgConfiguration)
        .map(extension -> (FeatureChangesPgConfiguration) extension)
        .findFirst();
  }

  private void subscribe(
      FeatureProvider provider,
      FeatureChangesPgConfiguration configuration,
      Supplier<Connection> connectionSupplier,
      Function<Connection, List<String>> notificationPoller) {
    subscriptions.put(provider.getId(), new ConcurrentHashMap<>());

    getSubscriptions(
            provider.info().getSchemas(),
            configuration.getListenForTypes(),
            connectionSupplier,
            notificationPoller)
        .forEach(subscription -> subscribe(provider.getId(), subscription));

    executorService.scheduleWithFixedDelay(
        () -> poll(provider.getId(), provider.changes()),
        configuration.getPollingInterval().toSeconds(),
        configuration.getPollingInterval().toSeconds(),
        TimeUnit.SECONDS);
  }

  private void subscribe(String provider, Subscription unconnected) {
    Subscription subscription = unconnected.connect();
    subscriptions.get(provider).put(subscription.getChannel(), subscription);

    try (Statement statement = subscription.getConnection().createStatement()) {
      String createFunction = subscription.getCreateFunction();
      String createTrigger = subscription.getCreateTrigger();
      String listen = subscription.getListen();

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Change listener function: \n{}", createFunction);
        LOGGER.trace("Change listener trigger: \n{}", createTrigger);
      }

      statement.execute(createFunction);
      statement.execute(createTrigger);
      statement.execute(listen);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Subscribed to feature changes for {}", subscription.getLabel());
      }
    } catch (SQLException e) {
      LogContext.error(
          LOGGER, e, "Could not subscribe to feature changes for {}", subscription.getLabel());
    }
  }

  private void poll(String provider, FeatureChanges featureChangeHandler) {
    if (subscriptions.containsKey(provider)) {
      subscriptions
          .get(provider)
          .forEach((channel, subscription) -> poll(provider, featureChangeHandler, subscription));
    }
  }

  private void poll(
      String provider, FeatureChanges featureChangeHandler, Subscription subscription) {
    if (!subscription.isConnected()) {
      LOGGER.debug("Lost connection to retrieve feature changes for {}", subscription.getLabel());
      subscriptions.get(provider).remove(subscription.getChannel());
      subscribe(provider, subscription);

      return;
    }

    for (String notification : subscription.pollNotifications()) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Feature change notification received: {}", notification);
      }
      onFeatureChange(subscription.getType(), featureChangeHandler, notification);
    }
  }

  private void onFeatureChange(
      String featureType, FeatureChanges featureChangeHandler, String payload) {
    try {
      FeatureChange featureChange = Notification.from(featureType, payload).asFeatureChange();

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Publishing feature change: {}", featureChange);
      }

      featureChangeHandler.handle(featureChange);
    } catch (Throwable e) {
      LogContext.errorAsInfo(
          LOGGER, e, "Could not parse feature change notification, the notification is ignored");
    }
  }

  private List<Subscription> getSubscriptions(
      Set<FeatureSchema> types,
      List<String> includes,
      Supplier<Connection> connectionSupplier,
      Function<Connection, List<String>> notificationPoller) {
    includes.forEach(
        include -> {
          if (types.stream()
              .map(FeatureSchema::getName)
              .noneMatch(type -> Objects.equals(type, include))) {
            LOGGER.warn("Change listener: unknown type '{}' in listenForTypes", include);
          }
        });

    final int[] count = {1};

    return types.stream()
        .filter(type -> includes.isEmpty() || includes.contains(type.getName()))
        .flatMap(
            type ->
                type.getEffectiveSourcePaths().stream()
                    .map(
                        sourcePath ->
                            ImmutableSubscription.builder()
                                .connectionFactory(connectionSupplier)
                                .notificationPoller(notificationPoller)
                                .index(count[0]++)
                                .type(type.getName())
                                .table(
                                    sourcePath.substring(
                                        1,
                                        sourcePath.contains("{")
                                            ? sourcePath.indexOf("{")
                                            : sourcePath.length()))
                                .idColumn(
                                    type.getIdProperty()
                                        .flatMap(FeatureSchema::getSourcePath)
                                        .orElseThrow())
                                .geometryColumn(
                                    type.getPrimaryGeometry()
                                        .map(s -> s.getSourcePath().orElseThrow()))
                                .intervalColumns(
                                    type.getPrimaryInterval()
                                        .map(
                                            t ->
                                                Tuple.of(
                                                    t.first().getSourcePath().orElseThrow(),
                                                    t.second().getSourcePath().orElseThrow())))
                                .instantColumn(
                                    type.getPrimaryInstant()
                                        .map(s -> s.getSourcePath().orElseThrow()))
                                .build()))
        .collect(Collectors.toList());
  }
}
