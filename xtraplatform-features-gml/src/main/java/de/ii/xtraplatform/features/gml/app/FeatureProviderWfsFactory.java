/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import dagger.assisted.AssistedFactory;
import de.ii.xtraplatform.base.domain.LogContext;
import de.ii.xtraplatform.entities.domain.AbstractEntityFactory;
import de.ii.xtraplatform.entities.domain.AutoEntityFactory;
import de.ii.xtraplatform.entities.domain.EntityData;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.entities.domain.EntityFactory;
import de.ii.xtraplatform.entities.domain.PersistentEntity;
import de.ii.xtraplatform.features.domain.ConnectorFactory;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.ImmutableProviderCommonData;
import de.ii.xtraplatform.features.domain.ProviderData;
import de.ii.xtraplatform.features.gml.domain.FeatureProviderWfsData;
import de.ii.xtraplatform.features.gml.domain.ImmutableFeatureProviderWfsData;
import de.ii.xtraplatform.features.gml.infra.WfsClientBasicFactoryDefault;
import de.ii.xtraplatform.features.gml.infra.WfsClientBasicFactorySimple;
import de.ii.xtraplatform.web.domain.Http;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@AutoBind
public class FeatureProviderWfsFactory
    extends AbstractEntityFactory<FeatureProviderDataV2, FeatureProviderWfs>
    implements EntityFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureProviderWfsFactory.class);

  private final FeatureProviderWfsAuto featureProviderWfsAuto;
  private final boolean skipHydration;

  @Inject
  public FeatureProviderWfsFactory(
      ConnectorFactory connectorFactory, ProviderWfsFactoryAssisted providerWfsFactoryAssisted) {
    super(providerWfsFactoryAssisted);
    this.featureProviderWfsAuto =
        new FeatureProviderWfsAuto(new WfsClientBasicFactoryDefault(connectorFactory));
    this.skipHydration = false;
  }

  public FeatureProviderWfsFactory(Http http) {
    super(null);
    this.featureProviderWfsAuto = new FeatureProviderWfsAuto(new WfsClientBasicFactorySimple(http));
    this.skipHydration = true;
  }

  @Override
  public String type() {
    return ProviderData.ENTITY_TYPE;
  }

  @Override
  public Optional<String> subType() {
    return Optional.of(FeatureProviderWfs.ENTITY_SUB_TYPE);
  }

  @Override
  public Class<? extends PersistentEntity> entityClass() {
    return FeatureProviderWfs.class;
  }

  @Override
  public EntityDataBuilder<FeatureProviderDataV2> dataBuilder() {
    return new ImmutableFeatureProviderWfsData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> superDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public EntityDataBuilder<FeatureProviderDataV2> emptyDataBuilder() {
    return new ImmutableFeatureProviderWfsData.Builder();
  }

  @Override
  public EntityDataBuilder<? extends EntityData> emptySuperDataBuilder() {
    return new ImmutableProviderCommonData.Builder();
  }

  @Override
  public Class<? extends EntityData> dataClass() {
    return FeatureProviderWfsData.class;
  }

  @Override
  public Optional<AutoEntityFactory> auto() {
    return Optional.of(featureProviderWfsAuto);
  }

  @Override
  public EntityData hydrateData(EntityData entityData) {
    FeatureProviderWfsData data = (FeatureProviderWfsData) entityData;

    if (skipHydration) {
      return data;
    }

    try {
      if (data.isAuto()) {
        LOGGER.info(
            "Feature provider with id '{}' is in auto mode, generating configuration ...",
            data.getId());

        Map<String, List<String>> types = featureProviderWfsAuto.analyze(data);

        data = featureProviderWfsAuto.generate(data, types, ignore -> {});
      }

      return data;

    } catch (Throwable e) {
      LogContext.error(
          LOGGER, e, "Feature provider with id '{}' could not be started", data.getId());
    }

    throw new IllegalStateException();
  }

  @AssistedFactory
  public interface ProviderWfsFactoryAssisted
      extends FactoryAssisted<FeatureProviderDataV2, FeatureProviderWfs> {
    @Override
    FeatureProviderWfs create(FeatureProviderDataV2 data);
  }
}
