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
 * @langDe Die Features sind in einem oder mehreren (Geo)Parquet-Dateien gespeichert.
 * @limitationsEn
 *     <p>The following limitations are known:
 *     <p><code>
 * - Only 2D geometries are supported.
 * - The option `linearizeCurves` is not supported. As described in the [GeoParquet specification](https://geoparquet.org/releases/v1.1.0/) all geometries must be encoded with WKB or according to the geometry types "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon" from the GeoArrow specification.
 * - The CQL2 functions `DIAMETER2D()` and `DIAMETER3D()` are not supported.
 * - CRUD operations are not supported.
 * - Columns with JSON content are not *yet* supported.
 * - When working with large files, the data might not be available immediately after application start, especially if no cache has been built beforehand. Requesting the data before it is ready may return `HTTP Error 503`. After some minutes the data should become available, the exact time depends on the size of the files.
 * - Populating a table with the content of multiple specific (Geo)Parquet files is not *yet* supported. However, it is possible to select multiple files using the `*` and `?` wildcard operators.
 * - The configuration is not checked for errors. This is especially the case for any S3-related configurations.
 * - The S3-access may fail, if S3-credentials are provided although the bucket is public.
 * - The behavior on dataset changes has not been tested.
 * </code>
 * @limitationsDe
 *     <p>Die folgenden Einschränkungen sind bekannt:
 *     <p><code>
 * - Es werden nur 2D-Geometrien unterstützt.
 * - Die Option `linearizeCurves` wird nicht unterstützt. Wie in der [GeoParquet Spezifikation](https://geoparquet.org/releases/v1.1.0/) beschrieben müssen alle Geometrien mit WKB oder gemäß den Geometrietypen "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon" aus der GeoArrow-Spezifikation kodiert werden.
 * - Die CQL2-Funktionen `DIAMETER2D()` und `DIAMETER3D()` werden nicht unterstützt.
 * - CRUD-Operationen werden nicht unterstützt.
 * - Spalten mit JSON-Inhalt werden *noch* nicht unterstützt.
 * - Bei großen Dateien sind die Daten möglicherweise nicht sofort nach dem Anwendungsstart verfügbar, insbesondere wenn zuvor kein Cache aufgebaut wurde. Werden die Daten abgefragt, bevor sie bereitstehen, kann dies zum Fehler `HTTP Error 503` führen. Nach einigen Minuten sollten die Daten verfügbar sein, die genaue Dauer hängt aber von der Größe der Dateien ab.
 * - Das Befüllen einer Tabelle mit dem Inhalt mehrerer spezifischer (Geo)Parquet-Dateien wird *noch* nicht unterstützt. Es ist jedoch möglich, mehrere Dateien mit den Wildcard-Operatoren `*` und `?` auszuwählen.
 * - Die Konfiguration wird nicht auf Korrektheit überprüft. Dies gilt insbesondere für die S3-Konfiguration.
 * - Der S3-Zugriff kann fehlschlagen, wenn S3-Zugangsdaten angegeben werden, obwohl der Bucket öffentlich ist.
 * - Das Verhalten bei Datensatzänderungen ist nicht getestet worden.
 * </code>
 * @cfgPropertiesAdditionalEn ### Connection Info
 *     <p>The connection info object for GeoParquet has the following properties:
 *     <p>{@docTable:connectionInfo}
 *     <p>### Table Mapping
 *     <p>To work with (Geo)Parquet files, it must be configured which tables exist and from which
 *     (Geo)Parquet files they are populated. For this, a mapping of table names to the (Geo)Parquet
 *     files with the corresponding data is required. This mapping is set in `driverOptions`. The
 *     following is important to note: <code>
 * - The table names must be in the `table` namespace by preceding them with `table.`, e.g. `table.FOO`.
 * - For S3, the path must be relative to the URL in `host`. For local files, it must be relative to the path set in `database`.
 * - Currently it is not possible to populate a table with the content of multiple specific files. However, the wildcard operators `*` and `?` can be used to select all files matching a specific pattern.
 * </code>
 *     <p>Examples: <code>
 * ```yaml
 * connectionInfo:
 *   driverOptions:
 *     table.FOO: "Unterordner/foo.parquet" # Match one specific file
 *     table.BAR: "Unterordner/Unterordner_2/*.parquet" # All parquet files inside Unterordner/Unterordner_2/
 *     table.FOOBAR: "*&#42;/*.parquet" # Match all parquet files at any depth
 * ```
 * </code> Finally the table names can be referenced in `sourcePath`: <code>
 * ```yaml
 * types:
 *   example:
 *     sourcePath: /FOOBAR
 * ```
 * </code>
 *     <p>### Configuration of S3
 *     <p>The possible parameters and their default values can be found in the [S3 documentation of
 *     DuckDB](https://duckdb.org/docs/current/core_extensions/httpfs/s3api#overview-of-s3-secret-parameters).
 *     The following must be noted: <code>
 * - Every value must be a String. This also applies to boolean parameters (in these cases use the Strings "true" and "false").
 * - The provided keys must be in lower-case, e.g. "endpoint" for the parameter `ENDPOINT`.
 * - The parameters `KEY_ID` and `SECRET` must be provided using `user` and `password` instead of `driverOptions`.
 * - The platform-specific secret type can be provided using the internal parameter "type". Valid options are "s3", "r2" and "gcs". For other providers use "s3" and set a custom endpoint instead.
 * - Instead of using the parameter `SCOPE` (which is not supported), set your sub-path as part of the bucket-URL in `host`.
 * </code>Example for `MinIO`: <code>
 * ```yaml
 * connectionInfo:
 *   host: "s3://geoparquet/subdirectory/"
 *   user: "KEY-ID"
 *   password: "SECRET KEY-ID"
 *   driverOptions:
 *     endpoint: "s3.minio-provider.net"
 *     use_ssl: "true"
 *     url_style: "path"
 *     table.TEST: "file.parquet"
 * ```
 * </code>
 * @cfgPropertiesAdditionalDe ### Connection Info
 *     <p>Das Connection-Info-Objekt für GeoParquet hat die folgenden Eigenschaften:
 *     <p>{@docTable:connectionInfo}
 *     <p>### Tabellenzuordnung
 *     <p>Um mit (Geo)Parquet-Dateien zu arbeiten, muss konfiguriert werden, welche Tabellen es gibt
 *     und mit dem Inhalt welcher (Geo)Parquet-Dateien diese befüllt werden. Dazu ist eine Zuordnung
 *     von Tabellennamen zu den (Geo)Parquet-Dateien mit den entsprechenden Daten erforderlich.
 *     Diese Zuordnung wird in `driverOptions` festgelegt. Folgendes ist zu beachten: <code>
 * - Die Tabellennamen müssen im `table` Namensraum liegen, indem ihnen `table.` vorangestellt wird z. B. `table.FOO`
 * - Bei S3 muss der Pfad relativ zur in `host` angegebenen URL sein. Bei lokalen Dateien muss er relativ zum Pfad in `database` sein.
 * - Derzeit ist es nicht möglich, eine Tabelle mit dem Inhalt mehrerer spezifischer Dateien zu befüllen. Die Wildcard-Operatoren `*` und `?` können jedoch verwendet werden, um Dateien nach einem bestimmten Muster auszuwählen.
 * </code> Beispiele: <code>
 * ```yaml
 * connectionInfo:
 *   driverOptions:
 *     table.FOO: "Unterordner/foo.parquet" # Wähle eine bestimmte Datei aus
 *     table.BAR: "Unterordner/Unterordner_2/*.parquet" # Alle Parquet-Dateien in Unterordner/Unterordner_2/
 *     table.FOOBAR: "*&#42;/*.parquet" # Alle Parquet-Dateien in beliebiger Tiefe
 * ```
 * </code>Die Tabellennamen können anschließend in `sourcePath` referenziert werden: <code>
 * ```yaml
 * types:
 *   beispiel:
 *     sourcePath: /FOOBAR
 * ```
 * </code>
 *     <p>### Konfiguration von S3
 *     <p>Die möglichen Parameter sowie Standardwerte sind in der [S3-Dokumentation von
 *     DuckDB](https://duckdb.org/docs/current/core_extensions/httpfs/s3api#overview-of-s3-secret-parameters)
 *     zu finden. Folgendes ist zu beachten: <code>
 * - Jeder Wert muss ein String sein. Dies gilt auch für boolesche Parameter (in diesem Fall "true" und "false" verwenden).
 * - Die Schlüssel müssen in Kleinbuchstaben angegeben werden, z.B. "endpoint" für den Parameter `ENDPOINT`.
 * - Die Parameter `KEY_ID` und `SECRET` müssen über `user` und `password` statt über `driverOptions` angegeben werden.
 * - Der plattformspezifische Secret-Typ kann über den internen Parameter "type" angegeben werden. Gültige Optionen sind "s3", "r2" und "gcs". Für andere Provider "s3" verwenden und den `ENDPOINT`-Parameter entsprechend setzen.
 * - Anstelle des Parameters `SCOPE` (der nicht unterstützt wird), den Unterordner als Teil der Bucket-URL in `host` angeben.
 * </code> Beispiel für `MinIO`: <code>
 * ```yaml
 * connectionInfo:
 *   host: "s3://geoparquet/Unterordner/"
 *   user: "KEY-ID"
 *   password: "SECRET KEY-ID"
 *   driverOptions:
 *     endpoint: "s3.minio-provider.net"
 *     use_ssl: "true"
 *     url_style: "path"
 *     table.TEST: "file.parquet"
 * ```
 * </code>
 * @ref:cfgProperties {@link
 *     de.ii.xtraplatform.features.geoparquet.domain.FeatureProviderGeoParquetDataDummy}
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
