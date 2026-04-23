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

/**
 * @title GeoParquet
 * @sortPriority 80
 * @langEn The features are stored using one or multiple (Geo)Parquet files.
 * @langDe Die Features sind in einem oder mehreren (Geo)Parquet Dateien gespeichert.
 * @limitationsEn
 *     <p>The following limitations are known:
 *     <p><code>
 *   - Files with 2 consecutive underscores may lead to unexpected behavior.
 *   - Only 2D geometries are supported.
 *   - The option `linearizeCurves` is not supported. All geometries must be encoded with WKB or according to the geometry types "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon" from the GeoArrow specification.
 *   - The CQL2 functions `DIAMETER2D()` and `DIAMETER3D()` are not supported.
 *   - CRUD operations are not supported.
 *   - Columns with JSON content are not *yet* supported.
 *     </code>
 * @limitationsDe
 *     <p>Die folgenden Einschränkungen sind bekannt:
 *     <p><code>
 *   - Dateien mit 2 aufeinanderfolgenden Unterstrichen können zu unerwartetem Verhalten führen.
 *   - Es werden nur 2D-Geometrien unterstützt.
 *   - Die Option `linearizeCurves` wird nicht unterstützt. Alle Geometrien müssen mit WKB oder gemäß den Geometrietypen "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon" aus der GeoArrow-Spezifikation kodiert werden.
 *   - Die CQL2-Funktionen `DIAMETER2D()` und `DIAMETER3D()` werden nicht unterstützt.
 *   - CRUD-Operationen werden nicht unterstützt.
 *   - Spalten mit JSON-Inhalt werden *noch* nicht unterstützt.
 * </code>
 * @cfgPropertiesAdditionalEn ### Connection Info
 *     <p>The connection info object for GeoParquet has the following properties:
 *     <p>{@docTable:connectionInfo}
 *     <p>### Path Syntax
 *     <p>The path to the required (Geo)Parquet file is specified in `sourcePath`. The path is
 *     relative to the directory specified in `database`, where all slashes except the first `/`
 *     must be replaced with `__` and the `.parquet` extension is omitted. Example: <code>
 *     Assuming the (Geo)Parquet files are located in `/resources/features/Datenordner` and `database` is set to `Datenordner` accordingly. For the file `/resources/features/Datenordner/Unterordner/geo_parquet_file.parquet` the path in `sourcePath` must be specified as `/Unterordner__geo_parquet_file`.
 *     </code>
 * @cfgPropertiesAdditionalDe ### Connection Info
 *     <p>Das Connection-Info-Objekt für GeoParquet wird wie folgt beschrieben:
 *     <p>{@docTable:connectionInfo}
 *     <p>### Pfadsyntax
 *     <p>Der Pfad zu der jeweils benötigten (Geo)Parquet-Datei wird in `sourcePath` angegeben. Der
 *     Pfad wird dabei relativ zu dem in `database` angegebenen Ordner angegeben, wobei alle
 *     Schrägstriche bis auf den ersten `/` durch `__` ersetzt werden müssen und die
 *     `.parquet`-Endung weggelassen wird. Beispiel: <code>
 *       Angenommen die (Geo)Parquet-Dateien liegen in `/resources/features/Datenordner` und `database` wird entsprechend mit `Datenordner` angegeben. Für die Datei `/resources/features/Datenordner/Unterordner/geo_parquet_file.parquet` muss der Pfad in `sourcePath` dann als `/Unterordner__geo_parquet_file` angegeben werden.
 *     </code>
 * @ref:connectionInfo {@link
 *     de.ii.xtraplatform.features.geoparquet.domain.ImmutableConnectionInfoGeoParquet}
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
    // ToDo: Remove check, because in the context of the GeoParquet Feature Provider dialects do not
    // exist
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
