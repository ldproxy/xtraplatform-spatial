/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocFile;
import de.ii.xtraplatform.docs.DocStep;
import de.ii.xtraplatform.docs.DocStep.Step;
import de.ii.xtraplatform.docs.DocTable;
import de.ii.xtraplatform.docs.DocTable.ColumnSet;
import de.ii.xtraplatform.entities.domain.ImmutableValidationResult;
import de.ii.xtraplatform.entities.domain.Mergeable;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

/**
 * # Transformations
 *
 * @langEn Transformations are supported in multiple parts of the configuration. Transformations do
 *     not affect data sources, they are applied on-the-fly as part of the encoding. Filter
 *     expressions do not take transformations into account, they have to be based on the source
 *     values.
 * @langDe Transformationen werden in verschiedenen Teilen der Konfiguration unterstützt. Die
 *     Transformation der Werte erfolgt bei der Aufbereitung der Daten für die Rückgabe. Die
 *     Datenhaltung selbst bleibt unverändert. Alle Filterausdrücke wirken unabhängig von etwaigen
 *     Transformationen bei der Ausgabe und müssen auf der Basis der Werte in der Datenhaltung
 *     formuliert sein - die Transformationen sind i.A. nicht umkehrbar und eine Berücksichtigung
 *     der inversen Transformationen bei Filterausdrücken wäre kompliziert und nur unvollständig
 *     möglich.
 * @langAll {@docTable:properties}
 * @langEn ## Examples for `stringFormat`
 *     <p><span v-pre>`https://example.com/id/kinder/kita/{{value}}`</span> inserts the value into
 *     the URI template.
 *     <p><span v-pre>`{{value | replace:'\\s*[0-9].*$':''}}`</span> removes all white space and
 *     numbers at the end (e.g. to remove a street number)
 *     <p><span v-pre>`{{value | replace:'^[^0-9]*':''}}`</span> removes everything before the first
 *     digit
 *     <p><span v-pre>`{{value | toUpper}}`</span> transforms the value to upper case
 *     <p><span v-pre>`{{value | toLower}}`</span> transforms the value to lower case
 *     <p><span v-pre>`{{value | urlEncode}}`</span> encodes special characters in the text for
 *     usage as aprt of an URI
 *     <p><span v-pre>`[{{value}}](https://de.wikipedia.org/wiki/{{value | replace:' ':'_' |
 *     urlencode}})`</span> transforms a value into a markdown link to a Wikipedia entry
 *     <p>
 * @langDe ## String-Template-Filter
 *     <p>Mit den Filtern können Strings nachprozessiert werden. Es können mehrere Filter
 *     nacheinander ausgeführt werden, jeweils durch ein '\|' getrennt.
 *     <p>Einige Beispiele:
 *     <p><span v-pre>`{{value | replace:'\\s*[0-9].*$':''}}`</span> entfernt alle Leerzeichen und
 *     Ziffern am Ende des Werts (z.B. zum Entfernen von Hausnummern)
 *     <p><span v-pre>`{{value | replace:'^[^0-9]*':''}}`</span> entfernt alle führenden Zeichen bis
 *     zur ersten Ziffer
 *     <p><span v-pre>`{{value | prepend:'(' | append:')'}}`</span> ergänzt Klammern um den Text
 *     <p><span v-pre>`{{value | toUpper}}`</span> wandelt den Text in Großbuchstaben um
 *     <p><span v-pre>`{{value | toLower}}`</span> wandelt den Text in Kleinbuchstaben um
 *     <p><span v-pre>`{{value | urlEncode}}`</span> kodiert Sonderzeichen im Text für die Nutzung
 *     in einer URI
 *     <p><span v-pre>`{{value | unHtml}}`</span> entfernt HTML-Tags (z.B. zum Reduzieren eines
 *     HTML-Links auf den Link-Text)
 *     <p><span v-pre>`[{{value}}](https://de.wikipedia.org/wiki/{{value | replace:' ':'_' |
 *     urlencode}})`</span> wandelt einen Gemeindenamen in einen Markdown-Link zum Wikipedia-Eintrag
 *     der Gemeinde
 *     <p>
 * @ref:properties {@link
 *     de.ii.xtraplatform.features.domain.transform.ImmutablePropertyTransformation}
 */
@DocFile(
    path = "providers/details",
    name = "transformations.md",
    tables = {
      @DocTable(
          name = "properties",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:properties}"),
            @DocStep(type = Step.JSON_PROPERTIES)
          },
          columnSet = ColumnSet.JSON_PROPERTIES),
    })
@Value.Immutable
@Value.Style(builder = "new")
@JsonDeserialize(builder = ImmutablePropertyTransformation.Builder.class)
public interface PropertyTransformation
    extends Buildable<PropertyTransformation>, Mergeable<PropertyTransformation> {

  abstract class Builder implements BuildableBuilder<PropertyTransformation> {}

  @Override
  default Builder getBuilder() {
    return new ImmutablePropertyTransformation.Builder().from(this);
  }

  /**
   * @langEn Rename a property.
   * @langDe Benennt die Eigenschaft auf den angegebenen Namen um.
   */
  Optional<String> getRename();

  @JsonIgnore
  Optional<String> getRenamePathOnly();

  /**
   * @langEn `IN_COLLECTION` (until version 3.0: `OVERVIEW`) skips the property only for the
   *     *Features* resource, `ALWAYS` always skips it, `NEVER` never skips it.
   * @langDe `IN_COLLECTION` (bis Version 3.0: `OVERVIEW`) unterdrückt die Objekteigenschaft bei der
   *     Features-Ressource (vor allem für die HTML-Ausgabe relevant), `ALWAYS` unterdrückt sie
   *     immer, `NEVER` nie.
   */
  Optional<String> getRemove();

  /**
   * @langEn Flattens object or array properties using the given separator. For arrays the property
   *     name is formed by the original property name followed by pairs of separator and array
   *     position. For objects the property name is formed by concatenating the original property
   *     separated by the given separator. Can only be applied on the feature level in the provider
   *     or using the wildcard property name `*` otherwise.
   * @langDe Flacht Objekt- oder Array-Properties unter Verwendung des gegebenen Trennzeichens ab.
   *     Ein Trennzeichen wird immer dann eingesetzt, wenn eine Eigenschaft multipel (ein Array)
   *     oder strukturiert (ein Objekt) ist. Im Fall eines Array ergeben sich die Namen der
   *     abgeflachten Eigenschaften aus dem Namen der Eigenschaft im Schema und der Position im
   *     Array, getrennt durch das Trennzeichen. Bei einer objektwertigen Eigenschaft ergeben sich
   *     die Namen der abgeflachten Eigenschaften aus dem Namen der objektwertigen Eigenschaft im
   *     Schema und den Namen der Eigenschaften im Datentyp des Objekts, ebenfalls getrennt durch
   *     das Trennzeichen. Erlaubt sind ".", "/", ":", oder "_". Kann nur auf der Feature-Ebene im
   *     Provider oder an anderen Stellen mittels des Wildcard Property-Names `*` angewendet werden.
   */
  Optional<String> getFlatten();

  /**
   * @langEn Remove properties with value `null`, including properties transformed with `nullify`.
   *     Can only be applied on the feature level in the provider or using the wildcard property
   *     name `*` otherwise.
   * @langDe Properties mit Wert `null` entfernen, inklusive Properties die mit `nullify`
   *     transformiert wurden. Kann nur auf der Feature-Ebene im Provider oder an anderen Stellen
   *     mittels des Wildcard Property-Names `*` angewendet werden.
   * @default true
   * @since v4.0
   */
  Optional<Boolean> getRemoveNullValues();

  /**
   * @langEn Reduces an object to a string using the same syntax as `stringFormat` but with
   *     additional replacements for the object property names.
   * @langDe Reduziert ein Objekt zu einem String mithilfe der `stringFormat`-Syntax aber mit
   *     zusätzlichen Ersetzungen für die Property-Names des Objekts.
   * @since v3.6
   */
  Optional<String> getObjectReduceFormat();

  /**
   * @langEn Reduces an object to one of its properties, the value is the desired property name.
   * @langDe Reduziert ein Objekt zu einem seiner Properties, der Wert ist der gewünschte
   *     Property-Name.
   * @since v3.6
   */
  Optional<String> getObjectReduceSelect();

  /**
   * @langEn Removes an object, if the property with the name in the value is `null`.
   * @langDe Entfernt ein Objekt, wenn die Eigenschaft mit dem Namen im Wert `null` ist.
   * @since v3.6
   */
  Optional<String> getObjectRemoveSelect();

  /**
   * @langEn Maps an object to another object, the value is map where the keys are the new property
   *     names. The values use the same syntax as `stringFormat` but with additional replacements
   *     for the source object property names.
   * @langDe Bildet ein Object auf ein anderes Object ab, der Wert ist eine Map bei der die Keys die
   *     neuen Property-Namen sind. Die Werte verwenden die `stringFormat`-Syntax aber mit
   *     zusätzlichen Ersetzungen für die Property-Names des Quell-Objekts.
   * @since v3.6
   */
  Map<String, String> getObjectMapFormat();

  @JsonIgnore
  Map<String, String> getObjectMapDuplicate();

  @JsonIgnore
  Map<String, String> getObjectAddConstants();

  /**
   * @langEn Reduces a value array to a string using the same syntax as `stringFormat` but with
   *     additional replacements for the array indexes.
   * @langDe Reduziert ein Werte-Array zu einem String mithilfe der `stringFormat`-Syntax aber mit
   *     zusätzlichen Ersetzungen für die Array-Indizes.
   * @since v3.6
   */
  Optional<String> getArrayReduceFormat();

  @JsonIgnore
  Optional<Boolean> getCoalesce();

  @JsonIgnore
  Optional<Boolean> getConcat();

  @JsonIgnore
  Optional<Type> getWrap();

  // Optional<String> getFlattenObjects();

  // Optional<String> getFlattenArrays();

  /**
   * @langEn Format a value, where `{{value}}` is replaced with the actual value and
   *     `{{serviceUrl}}` is replaced with the API landing page URI. Additonal operations can be
   *     applied to `{{value}}` by chaining them with `\|`, see the examples below.
   * @langDe Der Wert wird in den angegebenen neuen Wert transformiert. `{{value}}` wird dabei durch
   *     den aktuellen Wert und `{{serviceUr}}` durch die Landing-Page-URI der API ersetzt. Bei
   *     `{{value}}` können noch weitere [Filter](#String-Template-Filter) ausgeführt werden, diese
   *     werden durch "\|" getrennt. Diese Transformation ist nur bei STRING-wertigen Eigenschaften
   *     anwendbar. Ist der transformierte Wert für die HTML-Ausgabe gedacht, dann kann auch
   *     Markdown-Markup verwendet werden, dieser wird bei der HTML-Ausgabe aufbereitet.
   */
  Optional<String> getStringFormat();

  /**
   * @langEn Format date(-time) values with the given
   *     [pattern](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html#patterns),
   *     e.g. `dd.MM.yyyy` for German notation.
   * @langDe Der Wert wird unter Anwendung des angegebenen
   *     [Patterns](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html#patterns)
   *     transformiert. `dd.MM.yyyy` bildet den Wert zum Beispiel auf ein Datum nach deutscher
   *     Schreibweise ab. Diese Transformation ist nur bei DATETIME-wertigen Eigenschaften
   *     anwendbar.
   */
  Optional<String> getDateFormat();

  /**
   * @langEn Maps the value according to the given [codelist](../../auxiliaries/codelists.md). If
   *     the value is not found in the codelist or the codelist does not exist, the original value
   *     is passed through. Not applicable for properties containing objects. It is no longer
   *     recommended to specify `codelist` transformations in the feature provider or in the
   *     service. Instead, codelists associated with a property should be specified through the
   *     `constraints` of the property. Only the use in `constraints` enables the control of the
   *     representation of the values via the `profile` query parameter.
   * @langDe Bildet den Wert anhand der genannten [Codelist](../../auxiliaries/codelists.md) ab.
   *     Falls der Wert nicht in der Codelist enthalten ist oder die Codelist nicht gefunden wird,
   *     bleibt der Wert unverändert. Diese Transformation ist nicht bei objektwertigen
   *     Eigenschaften anwendbar. Es wird nicht mehr empfohlen, `codelist`-Transformationen im
   *     Feature-Provider oder im Service anzugeben. Stattdessen sollten Codelisten, die mit einer
   *     Eigenschaft verbunden sind, durch die `constraints` der Eigenschaft angegeben werden. Nur
   *     die Verwendung in `constraints` ermöglicht die Steuerung der Repräsentation der Werte über
   *     den `profile`-Query-Parameter.
   */
  Optional<String> getCodelist();

  /**
   * @langEn Maps the value according to the given map. If the value does not have a matching entry
   *     , the original value is passed through. A fallback mapping can be provided with the key
   *     `*`.
   * @langDe Bildet den Wert anhand der gegebenen Map ab. Falls der Wert keinen passenden Eintrag
   *     hat, bleibt der Wert unverändert. Ein Fallback-Mapping kann mit dem Schlüssel `*` angegeben
   *     werden.
   */
  Optional<Map<String, String>> getMap();

  /**
   * @langEn Maps all values matching the list of regular expressions to `null`. Not applicable for
   *     properties containing objects.
   * @langDe Bildet alle Werte, die einem der regulären Ausdrücke in der Liste entsprechen, auf
   *     `null` ab. Diese Transformation ist nicht bei objektwertigen Eigenschaften anwendbar.
   */
  List<String> getNullify();

  @JsonIgnore
  @Value.Derived
  default boolean isInternal() {
    return getWrap().isPresent();
  }

  @Override
  default PropertyTransformation mergeInto(PropertyTransformation source) {
    return new ImmutablePropertyTransformation.Builder()
        .from(source)
        .from(this)
        .nullify(
            Stream.concat(source.getNullify().stream(), getNullify().stream())
                .distinct()
                .collect(Collectors.toList()))
        .build();
  }

  default ImmutableValidationResult.Builder validate(
      ImmutableValidationResult.Builder builder,
      String collectionId,
      String property,
      Collection<String> codelists) {
    final Optional<String> remove = getRemove();
    if (remove.isPresent()) {
      if (!FeaturePropertyTransformerRemove.CONDITION_VALUES.contains(remove.get())) {
        builder.addStrictErrors(
            MessageFormat.format(
                "The remove transformation in collection ''{0}'' for property ''{1}'' is invalid. The value ''{2}'' is not one of the known values: {3}.",
                collectionId,
                property,
                remove.get(),
                FeaturePropertyTransformerRemove.CONDITION_VALUES));
      }
    }
    final Optional<String> dateFormat = getDateFormat();
    if (dateFormat.isPresent()) {
      try {
        LocalDate.now().format(DateTimeFormatter.ofPattern(dateFormat.get()));
      } catch (Exception e) {
        builder.addWarnings(
            MessageFormat.format(
                "The dateFormat transformation in collection ''{0}'' for property ''{1}'' with  value ''{2}'' is invalid, if used with a timestamp: {3}.",
                collectionId, property, dateFormat.get(), e.getMessage()));
      }
    }
    final Optional<String> codelist = getCodelist();
    if (codelist.isPresent()) {
      if (!codelists.contains(codelist.get())) {
        builder.addStrictErrors(
            MessageFormat.format(
                "The codelist transformation in collection ''{0}'' for property ''{1}'' is invalid. The codelist ''{2}'' is not one of the known values: {3}.",
                collectionId, property, codelist.get(), codelists));
      }
    }

    return builder;
  }
}
