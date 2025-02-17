/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.docs.DocMarker;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.entities.domain.EntityDataDefaults;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.features.domain.ExtensionConfiguration;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.WithConnectionInfo;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
@Value.Style(
    builder = "new",
    deepImmutablesDetection = true,
    attributeBuilderDetection = true,
    passAnnotations = DocIgnore.class)
@JsonDeserialize(builder = ImmutableFeatureProviderSqlData.Builder.class)
public interface FeatureProviderSqlData
    extends FeatureProviderDataV2, WithConnectionInfo<ConnectionInfoSql> {

  /**
   * @langEn See [Connection Info](#connection-info).
   * @langDe Siehe [Connection-Info](#connection-info).
   */
  @DocMarker("specific")
  @Nullable
  @Override
  ConnectionInfoSql getConnectionInfo();

  /**
   * @langEn Dataset change handling, for details see [Dataset Changes](10-sql.md#dataset-changes)
   *     below.
   * @langDe Behandlung von Datensatzänderungen, für Details siehe
   *     [Datensatzänderungen](10-sql.md#dataset-changes).
   * @since v4.3
   */
  @DocMarker("specific")
  @Nullable
  DatasetChangeSettings getDatasetChanges();

  /**
   * @langEn Defaults for the path expressions in `sourcePath`, for details see [Source Path
   *     Defaults](10-sql.md#source-path-defaults) below.
   * @langDe Defaults für die Pfad-Ausdrücke in `sourcePath`, für Details siehe
   *     [SQL-Pfad-Defaults](10-sql.md#source-path-defaults).
   */
  @DocMarker("specific")
  @Nullable
  SqlPathDefaults getSourcePathDefaults();

  /**
   * @langEn Options for query generation, for details see [Query
   *     Generation](10-sql.md#query-generation) below.
   * @langDe Einstellungen für die Query-Generierung, für Details siehe
   *     [Query-Generierung](10-sql.md#query-generation).
   */
  @DocMarker("specific")
  @Nullable
  QueryGeneratorSettings getQueryGeneration();

  // for json ordering
  @Override
  BuildableMap<FeatureSchema, ImmutableFeatureSchema.Builder> getTypes();

  // for json ordering
  @Override
  BuildableMap<FeatureSchema, ImmutableFeatureSchema.Builder> getFragments();

  @Value.Check
  default FeatureProviderSqlData initNestedDefault() {
    /*
     workaround for https://github.com/interactive-instruments/ldproxy/issues/225
     TODO: remove when fixed
    */
    if (Objects.isNull(getConnectionInfo())) {
      ImmutableFeatureProviderSqlData.Builder builder =
          new ImmutableFeatureProviderSqlData.Builder().from(this);
      builder.connectionInfoBuilder().database("");

      return builder.build();
    }

    return this;
  }

  @Value.Check
  default FeatureProviderSqlData mergeExtensions() {
    List<ExtensionConfiguration> distinctExtensions = getMergedExtensions();

    // remove duplicates
    if (getExtensions().size() > distinctExtensions.size()) {
      return new ImmutableFeatureProviderSqlData.Builder()
          .from(this)
          .extensions(distinctExtensions)
          .build();
    }

    return this;
  }

  abstract class Builder
      extends FeatureProviderDataV2.Builder<ImmutableFeatureProviderSqlData.Builder>
      implements EntityDataBuilder<FeatureProviderDataV2> {

    public abstract ImmutableFeatureProviderSqlData.Builder connectionInfo(
        ConnectionInfoSql connectionInfo);

    @Override
    public ImmutableFeatureProviderSqlData.Builder fillRequiredFieldsWithPlaceholders() {
      return this.id(EntityDataDefaults.PLACEHOLDER)
          .providerType(EntityDataDefaults.PLACEHOLDER)
          .providerSubType(EntityDataDefaults.PLACEHOLDER)
          .connectionInfo(
              new ImmutableConnectionInfoSql.Builder()
                  .database(EntityDataDefaults.PLACEHOLDER)
                  .build());
    }
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableQueryGeneratorSettings.Builder.class)
  interface QueryGeneratorSettings {

    @DocIgnore
    @Value.Default
    default int getChunkSize() {
      return 10000;
    }

    /**
     * @langEn Option to disable computation of the number of selected features for performance
     *     reasons that are returned in `numberMatched`. As a general rule this should be disabled
     *     for big datasets.
     * @langDe Steuert, ob bei Abfragen die Anzahl der selektierten Features berechnet und in
     *     `numberMatched` zurückgegeben werden soll oder ob dies aus Performancegründen
     *     unterbleiben soll. Bei großen Datensätzen empfiehlt es sich in der Regel, die Option zu
     *     deaktivieren.
     * @default true
     */
    @Value.Default
    default boolean getComputeNumberMatched() {
      return true;
    }

    // TODO
    @DocIgnore
    @Value.Default
    default Optional<String> getAccentiCollation() {
      return Optional.empty();
    }
  }

  @Value.Check
  default FeatureProviderSqlData migrateAssumeExternalChanges() {
    if (Objects.isNull(getDatasetChanges())
        && Objects.nonNull(getConnectionInfo())
        && getConnectionInfo().getAssumeExternalChanges()) {
      return new ImmutableFeatureProviderSqlData.Builder()
          .from(this)
          .datasetChanges(
              new ImmutableDatasetChangeSettings.Builder()
                  .mode(DatasetChangeSettings.Mode.EXTERNAL)
                  .build())
          .build();
    }

    return this;
  }

  /**
   * @langEn Defines how dataset changes should be detected. There are the following modes:
   *     <p><code>
   * - `OFF`: The dataset is considered static and no change detection is performed. If there is a
   *   new dataset version, that should result in changes to `connectionInfo`. (This will be the default in v5.x)
   * - `CRUD`: Changes to the dataset are exclusively made by the application itself through the CRUD interface. (This is the default in v4.x)
   * - `TRIGGER`: There are triggers set up for the dataset that notify the application about changes
   *   (see for example [Change Listener](90-extensions/change_listener.md)). Additionally a periodic synchronization can be configured with `syncPeriodic` for the case of missed notifications.
   * - `EXTERNAL`: Assume that the dataset may be changed by external applications.
   *   In this mode a synchronization happens on every provider start or reload.
   *   Additionally a periodic synchronization can be configured with `syncPeriodic`.
   *     </code>
   * @langDe Definiert wie Änderungen am Datensatz erkannt werden sollen. Es gibt die folgenden
   *     Modi:
   *     <p><code>
   * - `OFF`: Der Datensatz wird als statisch betrachtet und es wird keine Änderungserkennung durchgeführt.
   *   Wenn es eine neue Datensatzversion gibt, sollte diese zu Änderungen an `connectionInfo` führen. (Dies wird in v5.x der Standard sein)
   * - `CRUD`: Änderungen am Datensatz werden ausschließlich von der Anwendung selbst über die CRUD-Schnittstelle vorgenommen. (Dies ist in v4.x der Standard)
   * - `TRIGGER`: Es sind Trigger für den Datensatz eingerichtet, die die Anwendung über Änderungen informieren
   *   (siehe z.B. [Change Listener](90-extensions/change_listener.md)). Zusätzlich kann eine
   *   periodische Synchronisation mit `syncPeriodic` für den Fall von verpassten Benachrichtigungen konfiguriert werden.
   * - `EXTERNAL`: Annehmen, dass der Datensatz von externen Anwendungen geändert werden kann.
   *   In diesem Modus erfolgt eine Synchronisation bei jedem Start oder Reload des Providers.
   *   Zusätzlich kann eine periodische Synchronisation mit `syncPeriodic` konfiguriert werden.
   *     </code>
   */
  @Value.Immutable
  @JsonDeserialize(builder = ImmutableDatasetChangeSettings.Builder.class)
  interface DatasetChangeSettings {

    enum Mode {
      OFF,
      CRUD,
      TRIGGER,
      EXTERNAL,
    }

    /**
     * @langEn The mode for dataset change detection, see above.
     * @langDe Der Modus für die Erkennung von Datensatzänderungen, siehe oben.
     * @since v4.3
     * @default CRUD v4 \| OFF v5
     */
    @Value.Default
    default Mode getMode() {
      return Mode.CRUD;
    }

    /**
     * @langEn A crontab pattern to periodically refresh all cached data that is derived from the
     *     dataset. Only available for modes `EXTERNAL` and `TRIGGER`.
     * @langDe Ein Crontab-Pattern zum regelmäßigen Aktualisieren aller gecachten Daten, die sich
     *     aus dem Datensatz ableiten. Nur verfügbar für die Modi `EXTERNAL` und `TRIGGER`.
     * @since v4.3
     * @default null
     */
    @Nullable
    String getSyncPeriodic();

    @JsonIgnore
    @Value.Lazy
    default boolean isModeOff() {
      return getMode() == Mode.OFF;
    }

    @JsonIgnore
    @Value.Lazy
    default boolean isModeExternal() {
      return getMode() == Mode.EXTERNAL;
    }

    @JsonIgnore
    @Value.Lazy
    default boolean isModeCrud() {
      return getMode() == Mode.CRUD;
    }

    @JsonIgnore
    @Value.Lazy
    default boolean isModeTrigger() {
      return getMode() == Mode.TRIGGER;
    }
  }
}
