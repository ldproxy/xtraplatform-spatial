/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.graphql.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import dagger.Lazy;
import dagger.assisted.AssistedFactory;
import de.ii.xtraplatform.entities.domain.AbstractEntityFactory;
import de.ii.xtraplatform.entities.domain.EntityData;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.entities.domain.PersistentEntity;
import de.ii.xtraplatform.features.domain.ConnectorFactory;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableProviderCommonData;
import de.ii.xtraplatform.features.domain.ProviderData;
import de.ii.xtraplatform.features.domain.SchemaFragmentResolver;
import de.ii.xtraplatform.features.domain.SchemaReferenceResolver;
import de.ii.xtraplatform.features.graphql.domain.FeatureProviderGraphQlData;
import de.ii.xtraplatform.features.graphql.domain.ImmutableFeatureProviderGraphQlData;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class FeatureProviderGraphQlFactory
    extends AbstractEntityFactory<FeatureProviderDataV2, FeatureProviderGraphQl> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureProviderGraphQlFactory.class);

  private final Lazy<Set<SchemaFragmentResolver>> schemaResolvers;

  @Inject
  public FeatureProviderGraphQlFactory(
      Lazy<Set<SchemaFragmentResolver>> schemaResolvers,
      ConnectorFactory connectorFactory,
      ProviderGraphQlFactoryAssisted providerGraphQlFactoryAssisted) {
    super(providerGraphQlFactoryAssisted);
    this.schemaResolvers = schemaResolvers;
    Objects.requireNonNull(connectorFactory);
  }

  @Override
  public String type() {
    return ProviderData.ENTITY_TYPE;
  }

  @Override
  public Optional<String> subType() {
    return Optional.of(FeatureProviderGraphQl.ENTITY_SUB_TYPE);
  }

  @Override
  public Class<? extends PersistentEntity> entityClass() {
    return FeatureProviderGraphQl.class;
  }

  @Override
  public EntityDataBuilder<FeatureProviderDataV2> dataBuilder() {
    return new ImmutableFeatureProviderGraphQlData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> superDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> emptyDataBuilder() {
    return new ImmutableFeatureProviderGraphQlData.Builder();
  }

  @Override
  public Class<? extends EntityData> dataClass() {
    return FeatureProviderGraphQlData.class;
  }

  @Override
  public EntityData hydrateData(EntityData entityData) {
    FeatureProviderGraphQlData data = (FeatureProviderGraphQlData) entityData;

    SchemaReferenceResolver resolver = new SchemaReferenceResolver(data, schemaResolvers);

    if (resolver.needsResolving(data.getTypes())) {
      Map<String, FeatureSchema> types = resolver.resolve(data.getTypes());

      return new ImmutableFeatureProviderGraphQlData.Builder().from(data).types(types).build();
    }

    return data;
  }

  @AssistedFactory
  @FunctionalInterface
  public interface ProviderGraphQlFactoryAssisted
      extends FactoryAssisted<FeatureProviderDataV2, FeatureProviderGraphQl> {
    @Override
    FeatureProviderGraphQl create(FeatureProviderDataV2 data);
  }
}
