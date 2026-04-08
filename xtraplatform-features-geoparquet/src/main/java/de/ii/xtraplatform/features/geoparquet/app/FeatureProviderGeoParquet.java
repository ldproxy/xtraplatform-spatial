/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.geoparquet.app;

import dagger.assisted.Assisted;
import dagger.assisted.AssistedInject;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.cache.domain.Cache;
import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.crs.domain.CrsInfo;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.docs.DocDefs;
import de.ii.xtraplatform.docs.DocStep;
import de.ii.xtraplatform.docs.DocStep.Step;
import de.ii.xtraplatform.docs.DocTable;
import de.ii.xtraplatform.docs.DocTable.ColumnSet;
import de.ii.xtraplatform.entities.domain.Entity;
import de.ii.xtraplatform.entities.domain.Entity.SubType;
import de.ii.xtraplatform.features.domain.ConnectorFactory;
import de.ii.xtraplatform.features.domain.DecoderFactories;
import de.ii.xtraplatform.features.domain.FeatureProvider;
import de.ii.xtraplatform.features.domain.FeatureProviderConnector;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.ProviderData;
import de.ii.xtraplatform.features.domain.ProviderExtensionRegistry;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSql;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapters;
import de.ii.xtraplatform.features.sql.domain.SqlQueryBatch;
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import de.ii.xtraplatform.services.domain.Scheduler;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.values.domain.ValueStore;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DocDefs(
    tables = {
      @DocTable(
          name = "connectionInfo",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:connectionInfo}"),
            @DocStep(type = Step.JSON_PROPERTIES)
          },
          columnSet = ColumnSet.JSON_PROPERTIES),
    })
@Entity(
    type = ProviderData.ENTITY_TYPE,
    subTypes = {
      @SubType(key = ProviderData.PROVIDER_TYPE_KEY, value = FeatureProvider.PROVIDER_TYPE),
      @SubType(
          key = ProviderData.PROVIDER_SUB_TYPE_KEY,
          value = FeatureProviderGeoParquet.PROVIDER_SUB_TYPE)
    },
    data = FeatureProviderSqlData.class)
public class FeatureProviderGeoParquet extends FeatureProviderSql {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureProviderGeoParquet.class);

  public static final String ENTITY_SUB_TYPE = "feature/geoparquet";
  public static final String PROVIDER_SUB_TYPE = "GEOPARQUET";

  @AssistedInject
  public FeatureProviderGeoParquet(
      CrsTransformerFactory crsTransformerFactory,
      CrsInfo crsInfo,
      Cql cql,
      ConnectorFactory connectorFactory,
      SqlDbmsAdapters dbmsAdapters,
      Reactive reactive,
      ValueStore valueStore,
      ProviderExtensionRegistry extensionRegistry,
      DecoderFactories decoderFactories,
      VolatileRegistry volatileRegistry,
      Cache cache,
      Scheduler scheduler,
      @Assisted FeatureProviderDataV2 data) {
    super(
        crsTransformerFactory,
        crsInfo,
        cql,
        connectorFactory,
        dbmsAdapters,
        reactive,
        valueStore,
        extensionRegistry,
        decoderFactories,
        volatileRegistry,
        cache,
        scheduler,
        data,
        Map.of());
  }

  @Override
  protected boolean onStartup() throws InterruptedException {
    if (!Objects.equals(getData().getConnectionInfo().getDialect(), SqlDbmsAdapterDuckdb.ID)) {
      LOGGER.error(
          "Feature provider with id '{}' could not be started: dialect '{}' is not supported",
          getId(),
          getData().getConnectionInfo().getDialect());
      return false;
    }

    return super.onStartup();
  }

  @Override
  protected FeatureProviderConnector<SqlRow, SqlQueryBatch, SqlQueryOptions> createConnector(
      String providerSubType, String connectorId) {
    return super.createConnector(FeatureProviderSql.PROVIDER_SUB_TYPE, connectorId);
  }
}
