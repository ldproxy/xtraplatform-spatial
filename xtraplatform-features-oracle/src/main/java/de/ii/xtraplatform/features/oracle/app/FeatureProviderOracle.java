/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.oracle.app;

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

/**
 * @title Oracle
 * @sortPriority 60
 * @langEn The features are stored in an Oracle Spatial database.
 * @langDe Die Features sind in einer Oracle Spatial Datenbank gespeichert
 * @prerequisitesEn Since the Oracle JDBC driver is not open source, it is not included in the
 *     distribution. You have to download a recent `ojdbc11.jar` from the [Oracle
 *     website](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html) and add it
 *     to the [Store](../../application/20-configuration/10-store-new.md) as resource with type
 *     `oracle`.
 * @prerequisitesDe Da der Oracle JDBC-Treiber nicht Open Source ist, ist er nicht in der
 *     Distribution enthalten. Sie müssen ein aktuelles `ojdbc11.jar` von der
 *     [Oracle-Website](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)
 *     herunterladen und es als Ressource mit dem Typ `oracle` im
 *     [Store](../../application/20-configuration/10-store-new.md) hinzufügen.
 * @limitationsEn <code>
 * - Only **Oracle 19c** has been tested.
 * - Only geometries in a 2D coordinate reference system are supported.
 * - All identifiers must be unquoted identifiers; that is the identifiers will be all uppercase.
 * - The option `linearizeCurves` is not supported. All geometries must conform to the OGC Simple
 *   Feature Access standard.
 * - The spatial operator `S_CROSSES` is not supported.
 * - The CQL2 functions `DIAMETER2D()` and `DIAMETER3D()` are not supported.
 * - CRUD operations are not supported.
 * - Columns with JSON content are not supported.
 *     </code>
 * @limitationsDe <code>
 * - Es wurde nur **Oracle 19c** getestet.
 * - Es werden nur Geometrien in einem 2D-Koordinatenreferenzsystem unterstützt.
 * - Alle Bezeichner müssen nicht in Anführungszeichen gesetzt werden, d.h. die Bezeichner werden groß geschrieben.
 * - Die Option `linearizeCurves` wird nicht unterstützt. Alle Geometrien müssen Geometrien gemäß
 *   dem Standard OGC Simple Feature Access sein.
 * - Der räumliche Operator `S_CROSSES` wird nicht unterstützt.
 * - Die CQL2-Funktionen `DIAMETER2D()` und `DIAMETER3D()` werden nicht unterstützt.
 * - CRUD-Operationen werden nicht unterstützt.
 * - Spalten mit JSON-Inhalt werden nicht unterstützt.
 *     </code>
 * @cfgPropertiesAdditionalEn ### Connection Info
 *     <p>The connection info object for Oracle databases has the following properties:
 *     <p>{@docTable:connectionInfo}
 * @cfgPropertiesAdditionalDe ### Connection Info
 *     <p>Das Connection-Info-Objekt für Oracle-Datenbanken wird wie folgt beschrieben:
 *     <p>{@docTable:connectionInfo}
 * @ref:cfgProperties {@link de.ii.xtraplatform.features.sql.domain.ImmutableFeatureProviderSqlData}
 * @ref:connectionInfo {@link
 *     de.ii.xtraplatform.features.oracle.domain.ImmutableConnectionInfoOracle}
 */
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
          value = FeatureProviderOracle.PROVIDER_SUB_TYPE)
    },
    data = FeatureProviderSqlData.class)
public class FeatureProviderOracle extends FeatureProviderSql {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureProviderOracle.class);

  public static final String ENTITY_SUB_TYPE = "feature/oracle";
  public static final String PROVIDER_SUB_TYPE = "ORACLE";

  @AssistedInject
  public FeatureProviderOracle(
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
    if (!Objects.equals(getData().getConnectionInfo().getDialect(), SqlDbmsAdapterOras.ID)) {
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
