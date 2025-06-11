/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.entities.domain.AutoEntity;
import de.ii.xtraplatform.entities.domain.EntityDataBuilder;
import de.ii.xtraplatform.entities.domain.ValidationResult.MODE;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

public interface FeatureProviderDataV2 extends ProviderData, AutoEntity, ExtendableConfiguration {

  @JsonIgnore
  @Override
  @Value.Derived
  default long getEntitySchemaVersion() {
    return 2;
  }

  /**
   * @langEn Always `FEATURE`.
   * @langDe Stets `FEATURE`.
   */
  String getProviderType();

  /**
   * @langEn `SQL` for SQL DBMS as data source, `WFS` for *OGC Web Feature Service* as data source.
   * @langDe `SQL` für ein SQL-DBMS als Datenquelle, `WFS` für einen OGC Web Feature Service als
   *     Datenquelle.
   */
  @Override
  String getProviderSubType();

  /**
   * @langEn Coordinate reference system of geometries in the dataset. The EPSG code of the
   *     coordinate reference system is given as integer in `code`. `forceAxisOrder` may be set to
   *     use a non-default axis order: `LON_LAT` uses longitude/east as first value and
   *     latitude/north as second value, `LAT_LON` uses the reverse. `NONE` uses the default axis
   *     order and is the default value. Example: The default coordinate reference system `CRS84`
   *     would look like this: `code: 4326` and `forceAxisOrder: LON_LAT`.
   * @langDe Das Koordinatenreferenzsystem, in dem Geometrien in dem Datensatz geführt werden. Der
   *     EPSG-Code des Koordinatenreferenzsystems wird als Integer in `code` angegeben. Mit
   *     `forceAxisOrder` kann die Koordinatenreihenfolge geändert werden: `NONE` verwendet die
   *     Reihenfolge des Koordinatenreferenzsystems, `LON_LAT` verwendet stets Länge/Ostwert als
   *     ersten und Breite/Nordwert als zweiten Wert, `LAT_LON` entsprechend umgekehrt. Beispiel:
   *     Das Default-Koordinatenreferenzsystem `CRS84` entspricht `code: 4326` und `forceAxisOrder:
   *     LON_LAT`.
   * @default CRS84
   */
  Optional<EpsgCrs> getNativeCrs();

  /**
   * @langEn A timezone ID, such as `Europe/Berlin`. Is applied to temporal values without timezone
   *     in the dataset.
   * @langDe Eine Zeitzonen-ID, z.B. `Europe/Berlin`. Wird auf temporale Werte ohne Zeitzone im
   *     Datensatz angewendet.
   * @default UTC
   */
  @JsonDeserialize(converter = ZoneIdFromString.class)
  Optional<ZoneId> getNativeTimeZone();

  @DocIgnore
  Optional<String> getDefaultLanguage();

  /**
   * @langEn Optional type definition validation with regard to the data source (only for SQL).
   *     `NONE` means no validation. With `LAX` the validation will fail and the provider will not
   *     start, when issues are detected that would definitely lead to runtime errors. Issues that
   *     might lead to runtime errors depending on the data will be logged as warning. With `STRICT`
   *     the validation will fail for any detected issue. That means the provider will only start if
   *     runtime errors with regard to the data source can be ruled out.
   * @langDe Steuert ob die Spezifikationen der Objektarten daraufhin geprüft werden, ob sie zur
   *     Datenquelle passen (nur für SQL). `NONE` heißt keine Prüfung. Bei `LAX` schlägt die Prüfung
   *     fehl und der Start des Providers wird verhindert, wenn Probleme festgestellt werden, die in
   *     jedem Fall zu Laufzeitfehlern führen würden. Probleme die abhängig von den tatsächlichen
   *     Daten zu Laufzeitfehlern führen könnten, werden als Warnung geloggt. Bei `STRICT` führen
   *     alle festgestellten Probleme zu einem Fehlstart. Der Provider wird also nur gestartet, wenn
   *     keine Risiken für Laufzeitfehler im Zusammenhang mit der Datenquelle identifiziert werden.
   * @default NONE
   */
  @Nullable
  MODE getTypeValidation();

  /**
   * @langEn A template for schema labels. By default, the label set in the schema is used - or the
   *     name, if no label is set. The template is a string template and can contain the variables
   *     `{{value}}` for the schema label and `{{unit}}` for the unit declared in the schema. For
   *     example, to append the unit of measure in square brackets to the property label, one can
   *     use `{{value}}{{unit \| prepend:' [' \| append:']'}}`. This option is only supported in the
   *     SQL feature provider.
   * @langDe Ein Template für Labels im Schema. Standardmäßig wird das im Schema festgelegte Label
   *     verwendet - oder der Name, wenn kein Label festgelegt ist. Die Vorlage ist eine
   *     String-Vorlage und kann die Variablen `{{value}}` für die Schemabezeichnung und `{unit}}`
   *     für die im Schema deklarierte Maßeinheit enthalten. Um zum Beispiel die Einheit in eckigen
   *     Klammern an das Label einer Property anzuhängen, kann man `{{value}}{{unit \| prepend:' ['
   *     \| append:']'}}` verwenden. Diese Option wird nur im SQL Feature-Provider unterstützt.
   * @default `{{value}}`
   */
  Optional<String> getLabelTemplate();

  /**
   * @langEn Definition of extensions, see [Extensions](90-extensions/README.md).
   * @langDe Definition von Erweiterungen, siehe [Erweiterungen](90-extensions/README.md).
   * @default []
   */
  @Override
  List<ExtensionConfiguration> getExtensions();

  // TODO: rename manually to getInputSchemas/getRawSchemas with @JsonProperty("types"), return new
  // InputSchema
  // TODO: add new method getTypes with @JsonIgnore that returns the resolved schemas
  // TODO: in the end we need at least three schemas: input, internal, downstream

  /**
   * @langEn Definition of feature types. The entries have to be [schema
   *     definitions](#schema-definitions) with `type: OBJECT` and at least one property with `role:
   *     ID`.
   * @langDe Definition von Feature-Types. Die Einträge sind
   *     [Schema-Definitionen](#schema-definitions) mit `type: OBJECT` und mindestens einem Property
   *     mit `role: ID`.
   * @default {}
   */
  @JsonMerge
  @JsonProperty("types")
  BuildableMap<FeatureSchema, ImmutableFeatureSchema.Builder> getTypes();

  /**
   * @langEn Definition of reusable schema fragments that can be referenced using `schema` in
   *     `types`. The entries are arbitrary [schema definitions](#schema-definitions).
   * @langDe Definition von wiederverwendbaren Schema-Fragmenten, die mittels `schema` in `types`
   *     referenziert werden können. Die Einträge sind beliebige
   *     [Schema-Definitionen](#schema-definitions).
   * @default {}
   */
  @JsonMerge
  BuildableMap<FeatureSchema, ImmutableFeatureSchema.Builder> getFragments();

  @DocIgnore
  Map<String, Map<String, String>> getCodelists();

  /**
   * @langEn Option to derive `types` definitions automatically from the data source. When enabled
   *     `types` must not be set.
   * @langDe Steuert, ob die Informationen zu `types` beim Start automatisch aus der Datenquelle
   *     bestimmt werden sollen (Auto-Modus). In diesem Fall sollte `types` nicht angegeben sein.
   * @default false
   */
  @Override
  Optional<Boolean> getAuto();

  // custom builder to automatically use keys of types as name of FeatureTypeV2
  abstract class Builder<T extends Builder<T>> implements EntityDataBuilder<FeatureProviderDataV2> {

    @JsonIgnore
    public abstract Map<String, ImmutableFeatureSchema.Builder> getTypes();

    @JsonProperty(value = "types")
    public Map<String, ImmutableFeatureSchema.Builder> getTypes2() {
      Map<String, ImmutableFeatureSchema.Builder> types = getTypes();

      return new ApplyKeyToValueMap<>(types, (key, builder) -> builder.name(key));
    }

    public abstract T putTypes(String key, ImmutableFeatureSchema.Builder builder);

    @JsonProperty(value = "types")
    public T putTypes2(String key, ImmutableFeatureSchema.Builder builder) {
      return putTypes(key, builder.name(key));
    }

    @JsonIgnore
    public abstract Map<String, ImmutableFeatureSchema.Builder> getFragments();

    @JsonProperty(value = "fragments")
    public Map<String, ImmutableFeatureSchema.Builder> getFragments2() {
      Map<String, ImmutableFeatureSchema.Builder> types = getFragments();

      return new ApplyKeyToValueMap<>(types, (key, builder) -> builder.name(key));
    }

    public abstract T putFragments(String key, ImmutableFeatureSchema.Builder builder);

    @JsonProperty(value = "fragments")
    public T putFragments2(String key, ImmutableFeatureSchema.Builder builder) {
      return putFragments(key, builder.name(key));
    }

    public abstract T id(String id);

    public abstract T providerType(String providerType);

    public abstract T providerSubType(String providerSubType);

    // jackson should append to instead of replacing extensions
    @JsonIgnore
    public abstract T extensions(Iterable<? extends ExtensionConfiguration> elements);

    @JsonProperty("extensions")
    public abstract T addAllExtensions(Iterable<? extends ExtensionConfiguration> elements);
  }
}
