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
import de.ii.xtraplatform.entities.domain.maptobuilder.encoding.MergeableMapEncodingEnabled;
import de.ii.xtraplatform.features.domain.ConnectionInfo;
import de.ii.xtraplatform.features.sql.infra.db.SqlConnectorRx;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** */
@Value.Immutable
@Value.Style(
    builder = "new",
    deepImmutablesDetection = true,
    attributeBuilderDetection = true,
    passAnnotations = DocIgnore.class)
@JsonDeserialize(builder = ImmutableConnectionInfoSql.Builder.class)
@MergeableMapEncodingEnabled
public interface ConnectionInfoSql extends ConnectionInfo {

  enum Dialect {
    PGIS,
    GPKG
  }

  /**
   * @langEn Always `SLICK`.
   * @langDe Stets `SLICK`.
   */
  @JsonIgnore
  @Override
  @Value.Derived
  default String getConnectorType() {
    return SqlConnectorRx.CONNECTOR_TYPE;
  }

  /**
   * @langEn `PGIS` for PostgreSQL/PostGIS, `GPKG` for GeoPackage or SQLite/SpatiaLite.
   * @langDe `PGIS` für PostgreSQL/PostGIS, `GPKG` für GeoPackage oder SQLite/SpatiaLite.
   * @default PGIS
   */
  @Value.Default
  default String getDialect() {
    return SqlDbmsPgis.ID;
  }

  /**
   * @langEn The name of the database. For `GPKG` a relative path to a resource with type `features`
   *     in the [Store](../../application/20-configuration/10-store-new.md).
   * @langDe Der Name der Datenbank. Für `GPKG` ein relativer Pfad zu einer Ressource mit Typ
   *     `features` im [Store](../../application/20-configuration/10-store-new.md).
   */
  String getDatabase();

  /**
   * @langEn The database host. To use a non-default port, add it to the host separated by `:`, e.g.
   *     `db:30305`. Not relevant for `GPKG`.
   * @langDe Der Datenbankhost. Wird ein anderer Port als der Standardport verwendet, ist dieser
   *     durch einen Doppelpunkt getrennt anzugeben, z.B. `db:30305`. Nicht relevant für `GPKG`.
   */
  Optional<String> getHost();

  /**
   * @langEn The user name. Not relevant for `GPKG`.
   * @langDe Der Benutzername. Nicht relevant für `GPKG`.
   */
  Optional<String> getUser();

  /**
   * @langEn The base64 encoded password of the user. Not relevant for `GPKG`.
   * @langDe Das mit base64 kodierte Passwort des Benutzers. Nicht relevant für `GPKG`.
   */
  Optional<String> getPassword();

  /**
   * @langEn The names of database schemas that should be used in addition to `public`. Not relevant
   *     for `GPKG`.
   * @langDe Die Namen der Schemas in der Datenbank, auf die zugegriffen werden soll. Nicht relevant
   *     für `GPKG`.
   * @default []
   */
  List<String> getSchemas();

  /**
   * @langEn Connection pool settings, for details see [Pool](10-sql.md#pool) below.
   * @langDe Einstellungen für den Connection-Pool, für Details siehe [Pool](10-sql.md#pool).
   * @default see below
   */
  @Nullable
  PoolSettings getPool();

  /**
   * @langEn The default collation locale used by the database for string comparisons, e.g. `en` or
   *     `en-US` for English. The special value `C` is allowed for the `C` or `POSIX` collation.
   *     This will not change the database settings or the queries, but is needed for string
   *     comparisons in the application.
   * @langDe Die Standard Collation Locale, die von der Datenbank für Stringvergleiche verwendet
   *     wird, z.B. `de` oder `de-DE` für Deutsch. Der spezielle Wert `C` ist für die `C`- oder
   *     `POSIX`-Collation erlaubt. Dies ändert nicht die Datenbankeinstellungen oder die Queries,
   *     sondern wird nur für Stringvergleiche in der Anwendung benötigt.
   * @default en-US
   */
  @Value.Default
  default Optional<String> getDefaultCollation() {
    return Optional.of(Locale.US.toLanguageTag());
  }

  /**
   * @langEn Custom options for the JDBC driver. For `PGIS`, you might pass `gssEncMode`, `ssl`,
   *     `sslmode`, `sslcert`, `sslkey`, `sslrootcert` and `sslpassword`. For details see the
   *     [driver
   *     documentation](https://jdbc.postgresql.org/documentation/head/connect.html#connection-parameters).
   * @langDe Einstellungen für den JDBC-Treiber. Für `PGIS` werden `gssEncMode`, `ssl`, `sslmode`,
   *     `sslcert`, `sslkey`, `sslrootcert` und `sslpassword` durchgereicht. Für Details siehe die
   *     [Dokumentation des
   *     Treibers](https://jdbc.postgresql.org/documentation/head/connect.html#connection-parameters).
   * @default {}
   */
  Map<String, String> getDriverOptions();

  @DocIgnore
  Optional<FeatureActionTrigger> getTriggers();

  @Override
  @JsonIgnore
  @Value.Lazy
  default boolean isShared() {
    return Objects.nonNull(getPool()) && getPool().getShared();
  }

  @Override
  @JsonIgnore
  @Value.Lazy
  default String getDatasetIdentifier() {
    return String.format("%s/%s", getHost().orElse(""), getDatabase());
  }

  /**
   * @langEn *Deprecated* (replaced by `datasetChanges.mode`). Assume that the connected dataset may
   *     be changed by external applications. Setting this to `true` for example will recompute
   *     extents and counts on every provider start or reload.
   * @langDe *Deprecated* (ersetzt durch `datasetChanges.mode`). Annehmen, dass der verbundene
   *     Datensatz durch externe Applikationen geändert werden kann. Wenn diese Option auf `true`
   *     gesetzt wrid, werden z.B. Extents und Counts bei jedem Start oder Reload des Providers neu
   *     berechnet.
   * @since v4.0
   * @default false
   */
  @Deprecated(since = "4.3", forRemoval = true)
  @Value.Default
  default boolean getAssumeExternalChanges() {
    return false;
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutablePoolSettings.Builder.class)
  interface PoolSettings {

    /**
     * @langEn Maximum number of connections to the database. The default value is computed
     *     depending on the number of processor cores and the maximum number of joins per feature
     *     type in the [Types Configuration](README.md#schema-definitions). The default value is
     *     recommended for optimal performance under load. The smallest possible value also depends
     *     on the maximum number of joins per feature type, smaller values are rejected.
     * @langDe Steuert die maximale Anzahl von Verbindungen zur Datenbank. Der Default-Wert ist
     *     abhängig von der Anzahl der Prozessorkerne und der Anzahl der Joins in der
     *     [Types-Konfiguration](README.md#schema-definitions). Der Default-Wert wird für optimale
     *     Performanz unter Last empfohlen. Der kleinstmögliche Wert ist ebenfalls von der Anzahl
     *     der Joins abhängig, kleinere Werte werden zurückgewiesen.
     * @default dynamic
     */
    @Nullable
    Integer getMaxConnections();

    /**
     * @langEn Minimum number of connections to the database that are maintained.
     * @langDe Steuert die minimale Anzahl von Verbindungen zur Datenbank, die jederzeit offen
     *     gehalten werden.
     * @default maxConnections
     */
    @Nullable
    Integer getMinConnections();

    @DocIgnore
    @Nullable
    String getInitFailTimeout();

    /**
     * @langEn The maximum amount of time that a connection is allowed to sit idle in the pool. Only
     *     applies to connections beyond the `minConnections` limit. A value of 0 means that idle
     *     connections are never removed from the pool.
     * @langDe Die maximale Zeit die eine Connection unbeschäftigt im Pool verbleibt. Bezieht sich
     *     nur auf Connections über der `minConnections` Grenze. Ein Wert von `0` bedeutet, dass
     *     unbeschäftigte Connections niemals aus dem Pool entfernt werden.
     * @default 10m
     */
    @Nullable
    String getIdleTimeout();

    /**
     * @langEn Diagnostic option. If set to a duration of at least `2s`, a warning with the stack
     *     trace of the borrowing code is logged whenever a connection has been out of the pool for
     *     longer than that, and a second message is logged when such a connection is returned after
     *     all. A connection that is never returned only produces the first message, which is how a
     *     leak is told apart from a slow request. Long-running requests such as large exports or
     *     multi-action transactions legitimately hold a connection for a while, so choose a value
     *     above the longest expected request. `0` disables the check. The messages are written by
     *     the logger `com.zaxxer.hikari`, the warning at level `WARN` and the return message at
     *     level `INFO`; a logging configuration that restricts third-party loggers has to let them
     *     through.
     * @langDe Diagnoseoption. Bei einer Dauer von mindestens `2s` wird eine Warnung mit dem
     *     Stacktrace des anfordernden Codes protokolliert, sobald eine Verbindung länger als diese
     *     Dauer aus dem Pool entnommen ist, und eine zweite Meldung, wenn eine solche Verbindung
     *     doch noch zurückgegeben wird. Eine Verbindung, die nie zurückgegeben wird, erzeugt nur
     *     die erste Meldung; so lässt sich ein Leck von einer langsamen Anfrage unterscheiden.
     *     Langlaufende Anfragen wie große Exporte oder Transaktionen mit vielen Aktionen halten
     *     eine Verbindung berechtigterweise länger, der Wert sollte daher über der längsten
     *     erwarteten Anfrage liegen. `0` deaktiviert die Prüfung. Die Meldungen schreibt der Logger
     *     `com.zaxxer.hikari`, die Warnung mit Level `WARN` und die Rückgabemeldung mit Level
     *     `INFO`; eine Logging-Konfiguration, die Fremd-Logger einschränkt, muss sie durchlassen.
     * @default 0
     */
    @Nullable
    String getLeakDetectionThreshold();

    /**
     * @langEn If enabled for multiple providers with matching `host`, `database` and `user`, a
     *     single connection pool will be shared between these providers. If any of the other
     *     `connectionInfo` options do not match, the provider startup will fail.
     * @langDe Wenn `shared` für mehrere Provider mit übereinstimmenden `host`, `database` und
     *     `user` aktiviert ist, teilen sich diese Provider einen Connection-Pool. Wenn eine der
     *     anderen Optionen in `connectionInfo` nicht übereinstimmt, schlägt der Start des Providers
     *     fehl.
     * @default false
     */
    @Nullable
    Boolean getShared();
  }
}
