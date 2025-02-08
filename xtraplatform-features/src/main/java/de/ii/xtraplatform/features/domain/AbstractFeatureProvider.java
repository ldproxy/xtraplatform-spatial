/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.base.domain.resiliency.DelayedVolatile;
import de.ii.xtraplatform.base.domain.resiliency.Volatile2;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.codelists.domain.Codelist;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.entities.domain.AbstractPersistentEntity;
import de.ii.xtraplatform.entities.domain.ValidationResult;
import de.ii.xtraplatform.entities.domain.ValidationResult.MODE;
import de.ii.xtraplatform.features.app.FeatureChangeHandlerImpl;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureQueriesExtension.LIFECYCLE_HOOK;
import de.ii.xtraplatform.features.domain.FeatureStream.ResultBase;
import de.ii.xtraplatform.features.domain.ImmutableSchemaMapping.Builder;
import de.ii.xtraplatform.features.domain.SchemaBase.Scope;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.features.domain.transform.SchemaTransformerChain;
import de.ii.xtraplatform.features.domain.transform.WithScope;
import de.ii.xtraplatform.features.domain.transform.WithoutProperties;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Runner;
import de.ii.xtraplatform.streams.domain.Reactive.Stream;
import de.ii.xtraplatform.values.domain.Values;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFeatureProvider<
        T, U, V extends FeatureProviderConnector.QueryOptions, W extends SchemaBase<W>>
    extends AbstractPersistentEntity<FeatureProviderDataV2>
    implements FeatureProviderEntity, FeatureProvider, FeatureInfo, FeatureQueries {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFeatureProvider.class);
  protected static final WithScope WITH_SCOPE_RETURNABLE = new WithScope(Scope.RETURNABLE);
  protected static final WithScope WITH_SCOPE_QUERIES =
      new WithScope(EnumSet.of(SchemaBase.Scope.QUERYABLE, SchemaBase.Scope.SORTABLE));
  protected static final WithScope WITH_SCOPE_RECEIVABLE =
      new WithScope(SchemaBase.Scope.RECEIVABLE);

  private final ConnectorFactory connectorFactory;
  private final Reactive reactive;
  private final CrsTransformerFactory crsTransformerFactory;
  private final ProviderExtensionRegistry extensionRegistry;
  private final Values<Codelist> codelistStore;
  private final FeatureChanges changeHandler;
  private final ScheduledExecutorService delayedDisposer;
  private final VolatileRegistry volatileRegistry;
  private Reactive.Runner streamRunner;
  private final DelayedVolatile<FeatureProviderConnector<T, U, V>> connector;
  private boolean datasetChanged;
  private boolean datasetChangedForced;
  private String previousDataset;

  protected AbstractFeatureProvider(
      ConnectorFactory connectorFactory,
      Reactive reactive,
      CrsTransformerFactory crsTransformerFactory,
      ProviderExtensionRegistry extensionRegistry,
      Values<Codelist> codelistStore,
      FeatureProviderDataV2 data,
      VolatileRegistry volatileRegistry) {
    super(data, volatileRegistry);
    this.connectorFactory = connectorFactory;
    this.reactive = reactive;
    this.crsTransformerFactory = crsTransformerFactory;
    this.extensionRegistry = extensionRegistry;
    this.codelistStore = codelistStore;
    this.volatileRegistry = volatileRegistry;
    this.changeHandler = new FeatureChangeHandlerImpl();
    this.connector =
        new DelayedVolatile<>(
            volatileRegistry,
            String.format("connector.%s", data.getProviderSubType().toLowerCase()));
    this.delayedDisposer =
        MoreExecutors.getExitingScheduledExecutorService(
            (ScheduledThreadPoolExecutor)
                Executors.newScheduledThreadPool(
                    1, new ThreadFactoryBuilder().setNameFormat("entity.lifecycle-%d").build()));
  }

  @Override
  public String getType() {
    return ProviderData.ENTITY_TYPE;
  }

  @Override
  public FeatureProviderDataV2 getData() {
    return super.getData();
  }

  @Override
  protected State reconcileStateNoComponents(@Nullable String capability) {
    return State.AVAILABLE;
  }

  @Override
  protected boolean onStartup() throws InterruptedException {
    //TODO: call after addSubcomponent?
    onVolatileStart();

    addCapability(FeatureInfo.CAPABILITY);
    addCapability(FeatureChanges.CAPABILITY);
    if (queries().isSupported()) {
      addCapability(FeatureQueries.CAPABILITY);
    }
    if (extents().isSupported()) {
      addCapability(FeatureExtents.CAPABILITY);
    }
    if (passThrough().isSupported()) {
      addCapability(FeatureQueriesPassThrough.CAPABILITY);
    }
    if (mutations().isSupported()) {
      addCapability(FeatureTransactions.CAPABILITY);
    }
    if (crs().isSupported()) {
      addCapability(FeatureCrs.CAPABILITY);
    }
    if (metadata().isSupported()) {
      addCapability(FeatureMetadata.CAPABILITY);
    }
    if (multiQueries().isSupported()) {
      addCapability(MultiFeatureQueries.CAPABILITY);
    }
    addSubcomponent(crsTransformerFactory, FeatureCrs.CAPABILITY);
    addSubcomponent(
        connector,
        FeatureQueries.CAPABILITY,
        FeatureExtents.CAPABILITY,
        FeatureQueriesPassThrough.CAPABILITY,
        FeatureTransactions.CAPABILITY,
        FeatureMetadata.CAPABILITY,
        MultiFeatureQueries.CAPABILITY);

    this.datasetChanged =
        connector.isPresent() && !connector.get().isSameDataset(getConnectionInfo());
    this.datasetChangedForced = getConnectionInfo().getAssumeExternalChanges();
    this.previousDataset =
        Optional.ofNullable(connector.get())
            .map(FeatureProviderConnector::getDatasetIdentifier)
            .orElse("");
    boolean previousAlive = softClosePrevious(streamRunner, connector.get());
    boolean isShared = getConnectionInfo().isShared();
    String connectorId = getConnectorId(previousAlive, isShared);

    FeatureProviderConnector<T, U, V> connector1 =
        createConnector(getData().getProviderSubType(), connectorId);
    this.connector.set(connector1);

    // TODO: ignore when startupMode=ASYNC
    if (!getConnector().isConnected()) {
      connectorFactory.disposeConnector(connector1);

      Optional<Throwable> connectionError = getConnector().getConnectionError();
      String message = connectionError.map(Throwable::getMessage).orElse("unknown reason");
      LOGGER.error("Feature provider with id '{}' could not be started: {}", getId(), message);
      if (connectionError.isPresent() && LOGGER.isDebugEnabled()) {
        LOGGER.debug("Stacktrace:", connectionError.get());
      }
      // TODO: volatile defective
      return false;
    }

    if (isShared) {
      connectorFactory.onDispose(connector1, LogContext.withMdc(this::onSharedConnectorDispose));
    }

    Optional<String> runnerError = getRunnerError(getConnectionInfo());

    if (runnerError.isPresent()) {
      LOGGER.error(
          "Feature provider with id '{}' could not be started: {}", getId(), runnerError.get());
      // TODO: volatile defective
      return false;
    }

    this.streamRunner =
        reactive.runner(
            getData().getId(),
            getRunnerCapacity(getConnectionInfo()),
            getRunnerQueueSize(getConnectionInfo()));

    // TODO: validation does not make sense when startupMode=ASYNC, move to editor/CLI
    if (getTypeInfoValidator().isPresent() && getData().getTypeValidation() != MODE.NONE) {
      final boolean isSuccess = validate();

      if (!isSuccess) {
        LOGGER.error(
            "Feature provider with id '{}' could not be started: {} {}",
            getId(),
            getData().getTypeValidation().name().toLowerCase(),
            "validation failed");
        // TODO: volatile defective
        return false;
      }
    }

    addSubcomponent(Volatile2.available("base"), FeatureInfo.CAPABILITY, FeatureChanges.CAPABILITY);

    return true;
  }

  protected FeatureProviderConnector<T, U, V> createConnector(
      String providerSubType, String connectorId) {
    return (FeatureProviderConnector<T, U, V>)
        connectorFactory.createConnector(providerSubType, connectorId, getConnectionInfo());
  }

  @Override
  protected void onStarted() {
    super.onStarted();

    onStateChange(
        (from, to) -> {
          LOGGER.info("Feature provider with id '{}' state changed: {}", getId(), getState());
        },
        true);

    String startupInfo =
        getStartupInfo()
            .map(map -> String.format(" (%s)", map.toString().replace("{", "").replace("}", "")))
            .orElse("");

    LOGGER.info("Feature provider with id '{}' started successfully.{}", getId(), startupInfo);

    extensionRegistry
        .getAll()
        .forEach(
            extension -> {
              if (extension.isSupported(getConnector())) {
                extension.on(LIFECYCLE_HOOK.STARTED, this, getConnector());
                // TODO: addSubcomponent
              }
            });
  }

  @Override
  protected void onReloaded() {
    String startupInfo =
        getStartupInfo()
            .map(map -> String.format(" (%s)", map.toString().replace("{", "").replace("}", "")))
            .orElse("");

    LOGGER.info("Feature provider with id '{}' reloaded successfully.{}", getId(), startupInfo);

    if (datasetChanged || datasetChangedForced) {
      if (datasetChangedForced) {
        LOGGER.info("Dataset has changed (forced).");
      } else {
        LOGGER.info(
            "Dataset has changed ({} -> {}).",
            previousDataset,
            getConnectionInfo().getDatasetIdentifier());
      }
      changeHandler.handle(
          ImmutableDatasetChange.builder().featureTypes(getData().getTypes().keySet()).build());
    }
    this.datasetChanged = false;
  }

  @Override
  protected void onStopped() {
    if (connector.isPresent()) {
      connectorFactory.disposeConnector(connector.get());
    }
    LOGGER.info("Feature provider with id '{}' stopped.", getId());
  }

  @Override
  public Optional<EpsgCrs> getCrs() {
    return getData().getNativeCrs();
  }

  @Override
  public Optional<FeatureSchema> getSchema(String type) {
    return Optional.ofNullable(getData().getTypes().get(type));
  }

  @Override
  public Set<FeatureSchema> getSchemas() {
    return Set.copyOf(getData().getTypes().values());
  }

  private boolean softClosePrevious(
      Runner previousRunner, FeatureProviderConnector<T, U, V> previousConnector) {
    if (Objects.nonNull(previousConnector) || Objects.nonNull(previousRunner)) {
      if (previousRunner.getActiveStreams() > 0) {
        LOGGER.debug("Active streams found, keeping previous connection pool alive.");
        delayedDisposer.schedule(
            LogContext.withMdc(() -> softClosePrevious(previousRunner, previousConnector)),
            10,
            TimeUnit.SECONDS);
        return true;
      }

      if (Objects.nonNull(previousConnector)) {
        LOGGER.debug("Disposing previous connection pool.");
        connectorFactory.disposeConnector(previousConnector);
      }
      if (Objects.nonNull(previousRunner)) {
        try {
          previousRunner.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
    return false;
  }

  private void onSharedConnectorDispose() {
    if (((WithConnectionInfo<?>) getData()).getConnectionInfo().isShared()
        && getEntityState() != STATE.RELOADING) {
      LOGGER.debug("Shared connector has changed, reloading.");
      doReload();
    }
  }

  private String getConnectorId(boolean previousAlive, boolean isShared) {
    Optional<Integer> previousIteration =
        previousAlive
            ? connector.get().getProviderId().contains(".")
                ? Optional.of(
                        connector
                            .get()
                            .getProviderId()
                            .substring(connector.get().getProviderId().lastIndexOf('.') + 1))
                    .flatMap(
                        i -> {
                          try {
                            return Optional.of(Integer.parseInt(i));
                          } catch (Throwable e) {
                          }
                          return Optional.of(1);
                        })
                : Optional.of(1)
            : Optional.empty();

    return String.format(
        "%s%s%s",
        getData().getId(),
        isShared ? ".shared" : "",
        previousIteration.map(i -> "." + (i + 1)).orElse(""));
  }

  private boolean validate() throws InterruptedException {
    boolean isSuccess = true;

    try {
      for (Map.Entry<String, List<W>> sourceSchema : getSourceSchemas().entrySet()) {
        LOGGER.info(
            "Validating type '{}' ({})",
            sourceSchema.getKey(),
            getData().getTypeValidation().name().toLowerCase());

        ValidationResult result =
            getTypeInfoValidator()
                .get()
                .validate(
                    sourceSchema.getKey(), sourceSchema.getValue(), getData().getTypeValidation());

        isSuccess = isSuccess && result.isSuccess();

        result.getErrors().forEach(LOGGER::error);
        result
            .getStrictErrors()
            .forEach(result.getMode() == MODE.STRICT ? LOGGER::error : LOGGER::warn);
        result.getWarnings().forEach(LOGGER::warn);

        checkForStartupCancel();
      }
    } catch (Throwable e) {
      if (e instanceof InterruptedException) {
        throw e;
      }
      LogContext.error(LOGGER, e, "Cannot validate types");
      isSuccess = false;
    }

    return isSuccess;
  }

  @Override
  protected void onStartupFailure(Throwable throwable) {
    LogContext.error(
        LOGGER, throwable, "Feature provider with id '{}' could not be started", getId());
  }

  protected int getRunnerCapacity(ConnectionInfo connectionInfo) {
    return Reactive.Runner.DYNAMIC_CAPACITY;
  }

  protected int getRunnerQueueSize(ConnectionInfo connectionInfo) {
    return Reactive.Runner.DYNAMIC_CAPACITY;
  }

  protected Optional<String> getRunnerError(ConnectionInfo connectionInfo) {
    return Optional.empty();
  }

  protected ConnectionInfo getConnectionInfo() {
    return ((WithConnectionInfo<?>) getData()).getConnectionInfo();
  }

  protected abstract Map<String, List<W>> getSourceSchemas();

  protected abstract FeatureQueryEncoder<U, V> getQueryEncoder();

  protected FeatureProviderConnector<T, U, V> getConnector() {
    return Objects.requireNonNull(connector.get());
  }

  protected Reactive.Runner getStreamRunner() {
    return streamRunner;
  }

  protected Optional<Map<String, String>> getStartupInfo() {
    return Optional.empty();
  }

  protected Optional<SourceSchemaValidator<W>> getTypeInfoValidator() {
    return Optional.empty();
  }

  protected abstract FeatureTokenDecoder<
          T, FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      getDecoder(Query query, Map<String, SchemaMapping> mappings);

  protected FeatureTokenDecoder<
          T, FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      getDecoderPassThrough(Query query) {
    return getDecoder(query, Map.of());
  }

  protected List<FeatureTokenTransformer> getDecoderTransformers() {
    return ImmutableList.of();
  }

  protected Map<String, Codelist> getCodelists() {
    return codelistStore.asMap();
  }

  @Override
  public final FeatureProviderCapabilities getCapabilities() {
    return getQueryEncoder().getCapabilities();
  }

  // TODO: throw exception when connector absent
  @Override
  public FeatureStream getFeatureStream(FeatureQuery query) {
    validateQuery(query);

    Query query2 = preprocessQuery(query);

    return new FeatureStreamImpl(
        query2,
        getData(),
        crsTransformerFactory,
        getCodelists(),
        this::runQuery,
        !query.hitsOnly());
  }

  // TODO: more tests
  protected final void validateQuery(Query query) {
    if (query instanceof FeatureQuery) {
      if (!getSourceSchemas().containsKey(((FeatureQuery) query).getType())) {
        throw new IllegalArgumentException("No features available for type");
      }
    }
    if (query instanceof MultiFeatureQuery) {
      for (TypeQuery typeQuery : ((MultiFeatureQuery) query).getQueries()) {
        if (!getSourceSchemas().containsKey(typeQuery.getType())) {
          throw new IllegalArgumentException("No features available for type");
        }
      }
    }
  }

  protected Query preprocessQuery(Query query) {
    return query;
  }

  @Override
  public FeatureChanges changes() {
    return changeHandler;
  }

  // TODO: encodingOptions vs executionOptions
  private FeatureTokenSource getFeatureTokenSource(
      Query query,
      Map<String, PropertyTransformations> propertyTransformations,
      Map<String, String> beforeHookResults,
      boolean passThrough) {
    TypeQuery typeQuery =
        query instanceof MultiFeatureQuery
            ? ((MultiFeatureQuery) query).getQueries().get(0)
            : (FeatureQuery) query;

    getQueryEncoder().validate(typeQuery, query);

    U transformedQuery = getQueryEncoder().encode(query, beforeHookResults);
    // TODO: remove options, already embedded in SqlQuerySet
    V options = getQueryEncoder().getOptions(typeQuery, query);
    Reactive.Source<T> source = getConnector().getSourceStream(transformedQuery, options);

    FeatureTokenDecoder<
            T,
            FeatureSchema,
            SchemaMapping,
            FeatureEventHandler.ModifiableContext<FeatureSchema, SchemaMapping>>
        decoder =
            passThrough
                ? getDecoderPassThrough(query)
                : getDecoder(query, createMapping(query, propertyTransformations));

    FeatureTokenSource featureSource = source.via(decoder);

    for (FeatureTokenTransformer transformer : getDecoderTransformers()) {
      featureSource = featureSource.via(transformer);
    }

    return featureSource;
  }

  private Map<String, SchemaMapping> createMapping(
      Query query, Map<String, PropertyTransformations> propertyTransformations) {
    if (query instanceof FeatureQuery) {
      FeatureQuery featureQuery = (FeatureQuery) query;

      WithScope withScope =
          featureQuery.getSchemaScope() == SchemaBase.Scope.RETURNABLE
              ? WITH_SCOPE_RETURNABLE
              : WITH_SCOPE_RECEIVABLE;

      return Map.of(
          featureQuery.getType(), createMapping(featureQuery, withScope, propertyTransformations));
    }

    if (query instanceof MultiFeatureQuery) {
      return ((MultiFeatureQuery) query)
          .getQueries().stream()
              .map(
                  typeQuery ->
                      Map.entry(
                          typeQuery.getType(),
                          createMapping(typeQuery, WITH_SCOPE_RETURNABLE, propertyTransformations)))
              .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
    }

    return Map.of();
  }

  private SchemaMapping createMapping(
      TypeQuery query,
      WithScope withScope,
      Map<String, PropertyTransformations> propertyTransformations) {
    SchemaTransformerChain schemaTransformations =
        propertyTransformations
            .get(query.getType())
            .getSchemaTransformations(
                null,
                !(query instanceof FeatureQuery) || !((FeatureQuery) query).returnsSingleFeature());

    return new Builder()
        .targetSchema(
            getData()
                .getTypes()
                .get(query.getType())
                .accept(withScope)
                .accept(schemaTransformations)
                .accept(new WithoutProperties(query.getFields(), query.skipGeometry())))
        .sourcePathTransformer(this::applySourcePathDefaults)
        .build();
  }

  protected String applySourcePathDefaults(String path, boolean isValue) {
    return path;
  }

  // TODO: throw exception when connector absent
  protected <W extends ResultBase> CompletionStage<W> runQuery(
      BiFunction<FeatureTokenSource, Map<String, String>, Stream<W>> stream,
      Query query,
      Map<String, PropertyTransformations> propertyTransformations,
      boolean passThrough) {
    Map<String, String> beforeHookResults = beforeQuery(query);

    FeatureTokenSource tokenSource =
        getFeatureTokenSource(query, propertyTransformations, beforeHookResults, passThrough);

    Reactive.RunnableStream<W> runnableStream =
        stream.apply(tokenSource, beforeHookResults).on(streamRunner);

    return runnableStream.run().whenComplete((result, throwable) -> afterQuery(query));
  }

  private Map<String, String> beforeQuery(Query query) {
    Map<String, String> beforeHookResults = new HashMap<>();

    extensionRegistry
        .getAll()
        .forEach(
            extension -> {
              if (extension.isSupported(getConnector())) {
                extension.on(
                    FeatureQueriesExtension.QUERY_HOOK.BEFORE,
                    getData(),
                    getConnector(),
                    query,
                    beforeHookResults::put);
              }
            });

    return beforeHookResults;
  }

  private void afterQuery(Query query) {
    extensionRegistry
        .getAll()
        .forEach(
            extension -> {
              if (extension.isSupported(getConnector())) {
                extension.on(
                    FeatureQueriesExtension.QUERY_HOOK.AFTER,
                    getData(),
                    getConnector(),
                    query,
                    (a, t) -> {});
              }
            });
  }
}
