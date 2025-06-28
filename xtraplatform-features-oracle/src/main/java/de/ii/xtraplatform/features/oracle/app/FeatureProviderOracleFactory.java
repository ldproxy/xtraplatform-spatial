/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.oracle.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import dagger.Lazy;
import dagger.assisted.AssistedFactory;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.entities.domain.AbstractEntityFactory;
import de.ii.xtraplatform.entities.domain.AutoEntityFactory;
import de.ii.xtraplatform.entities.domain.EntityData;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.entities.domain.EntityFactory;
import de.ii.xtraplatform.entities.domain.PersistentEntity;
import de.ii.xtraplatform.entities.domain.ValidationResult.MODE;
import de.ii.xtraplatform.features.domain.ConstantsResolver;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableProviderCommonData;
import de.ii.xtraplatform.features.domain.MappingOperationResolver;
import de.ii.xtraplatform.features.domain.ProviderData;
import de.ii.xtraplatform.features.domain.SchemaFragmentResolver;
import de.ii.xtraplatform.features.domain.SchemaReferenceResolver;
import de.ii.xtraplatform.features.domain.TypesResolver;
import de.ii.xtraplatform.features.domain.transform.DefaultRolesResolver;
import de.ii.xtraplatform.features.domain.transform.FeatureRefEmbedder;
import de.ii.xtraplatform.features.domain.transform.FeatureRefResolver;
import de.ii.xtraplatform.features.domain.transform.ImplicitMappingResolver;
import de.ii.xtraplatform.features.domain.transform.LabelTemplateResolver;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData.DatasetChangeMode;
import de.ii.xtraplatform.features.sql.domain.ImmutableConnectionInfoSql;
import de.ii.xtraplatform.features.sql.domain.ImmutableDatasetChangeSettings;
import de.ii.xtraplatform.features.sql.domain.ImmutableFeatureProviderSqlData;
import de.ii.xtraplatform.features.sql.domain.ImmutableFeatureProviderSqlData.Builder;
import de.ii.xtraplatform.features.sql.domain.ImmutablePoolSettings;
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings;
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults;
import de.ii.xtraplatform.features.sql.domain.SqlClientBasicFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class FeatureProviderOracleFactory
    extends AbstractEntityFactory<FeatureProviderDataV2, FeatureProviderOracle>
    implements EntityFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureProviderOracleFactory.class);

  private final Lazy<Set<SchemaFragmentResolver>> schemaResolvers;
  private final boolean skipHydration;
  private final Set<String> connectors;

  @Inject
  public FeatureProviderOracleFactory(
      Lazy<Set<SchemaFragmentResolver>> schemaResolvers,
      ProviderOracleFactoryAssisted providerOracleFactoryAssisted) {
    super(providerOracleFactoryAssisted);
    this.schemaResolvers = schemaResolvers;
    this.skipHydration = false;
    this.connectors = Set.of();
  }

  // for ldproxy-cfg
  public FeatureProviderOracleFactory(SqlClientBasicFactory sqlClientBasicFactory) {
    super(null);
    this.schemaResolvers = null;
    this.skipHydration = true;
    this.connectors = Set.of();
  }

  @Override
  public String type() {
    return ProviderData.ENTITY_TYPE;
  }

  @Override
  public Optional<String> subType() {
    return Optional.of(FeatureProviderOracle.ENTITY_SUB_TYPE);
  }

  @Override
  public Class<? extends PersistentEntity> entityClass() {
    return FeatureProviderOracle.class;
  }

  @Override
  public EntityDataBuilder<FeatureProviderDataV2> dataBuilder() {
    return new ImmutableFeatureProviderSqlData.Builder()
        .typeValidation(MODE.NONE)
        .datasetChanges(
            new ImmutableDatasetChangeSettings.Builder().mode(DatasetChangeMode.OFF).build())
        .sourcePathDefaults(new ImmutableSqlPathDefaults.Builder().build())
        .queryGeneration(new ImmutableQueryGeneratorSettings.Builder().build())
        .connectionInfo(
            new ImmutableConnectionInfoSql.Builder()
                .database("")
                .dialect(SqlDbmsAdapterOras.ID)
                .pool(
                    new ImmutablePoolSettings.Builder()
                        .maxConnections(-1)
                        .minConnections(1)
                        .idleTimeout("10m")
                        .shared(false)
                        .build())
                .build());
  }

  @Override
  public EntityDataBuilder<? extends EntityData> superDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public EntityDataBuilder<FeatureProviderDataV2> emptyDataBuilder() {
    return new ImmutableFeatureProviderSqlData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> emptySuperDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public Class<? extends EntityData> dataClass() {
    return FeatureProviderSqlData.class;
  }

  @Override
  public Optional<AutoEntityFactory> auto() {
    return Optional.empty();
  }

  @Override
  public EntityData hydrateData(EntityData entityData) {
    FeatureProviderSqlData data = (FeatureProviderSqlData) entityData;

    if (skipHydration) {
      return entityData;
    }

    try {
      List<TypesResolver> resolvers =
          List.of(
              new SchemaReferenceResolver(data, schemaResolvers),
              new MappingOperationResolver(true),
              new FeatureRefEmbedder(data.getId()),
              new FeatureRefResolver(connectors),
              new ImplicitMappingResolver(),
              new ConstantsResolver(),
              new LabelTemplateResolver(data.getLabelTemplate()),
              new MappingOperationResolver(),
              new DefaultRolesResolver());

      for (TypesResolver resolver : resolvers) {
        data = applyTypesResolver(data, resolver);
      }

      return data;

    } catch (Throwable e) {
      LogContext.error(
          LOGGER, e, "Feature provider with id '{}' could not be started", data.getId());
    }

    throw new IllegalStateException();
  }

  @Override
  public Map<String, String> getListEntryKeys() {
    return Map.of("extensions", "type");
  }

  private FeatureProviderSqlData applyTypesResolver(
      FeatureProviderSqlData data, TypesResolver resolver) {
    Map<String, FeatureSchema> types = data.getTypes();
    int rounds = 0;

    while (resolver.needsResolving(types)) {
      types = resolver.resolve(types);
      if (++rounds >= resolver.maxRounds()) {
        resolver.maxRoundsWarning().ifPresent(LOGGER::warn);
        break;
      }
    }

    if (rounds > 0) {
      return new Builder().from(data).types(types).fragments(Map.of()).build();
    }

    return data;
  }

  @AssistedFactory
  public interface ProviderOracleFactoryAssisted
      extends FactoryAssisted<FeatureProviderDataV2, FeatureProviderOracle> {
    @Override
    FeatureProviderOracle create(FeatureProviderDataV2 data);
  }
}
