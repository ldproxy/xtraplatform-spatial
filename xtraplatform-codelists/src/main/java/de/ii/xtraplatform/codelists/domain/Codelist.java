/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.codelists.domain;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import de.ii.xtraplatform.docs.DocFile;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.docs.DocStep;
import de.ii.xtraplatform.docs.DocStep.Step;
import de.ii.xtraplatform.docs.DocTable;
import de.ii.xtraplatform.docs.DocTable.ColumnSet;
import de.ii.xtraplatform.values.domain.StoredValue;
import de.ii.xtraplatform.values.domain.ValueBuilder;
import de.ii.xtraplatform.values.domain.annotations.FromValueStore;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * @langEn # Codelists
 *     <p>Codelists allow to map property values to a different value. This is useful especially for
 *     HTML representations.
 * @langDe # Codelisten
 *     <p>Codelisten können zum Übersetzen von Eigenschaftswerten in einen anderen Wert genutzt
 *     werden, meist für die HTML-Ausgabe.
 * @langEn ## Configuration
 *     <p>The following table describes the structure of the code list files.
 *     <p>{@docTable:properties}
 *     <p>For the target values in `entries` and for `fallback` also
 *     [stringFormat](../providers/details/transformations.md) transformations can be used. If the
 *     transformed value is intended for HTML output, then Markdown markup can also be used, this
 *     will be formatted in the HTML output.
 * @langDe ## Konfiguration
 *     <p>Die nachfolgende Tabelle beschreibt die Struktur der Codelisten-Dateien.
 *     <p>{@docTable:properties}
 *     <p>Bei den Zielwerten in `entries` und bei `fallback` können auch
 *     [stringFormat](../providers/details/transformations.md)-Transformationen genutzt werden. Ist
 *     der transformierte Wert für die HTML-Ausgabe gedacht, dann kann auch Markdown-Markup
 *     verwendet werden, dieser wird bei der HTML-Ausgabe aufbereitet.
 * @langEn ### Example
 *     <p>Based on the INSPIRE codelist
 *     [EnvironmentalDomain](https://inspire.ec.europa.eu/codeList/EnvironmentalDomain), maps values
 *     like `soil` to the German label of the entry in the INSPIRE codelist registry.
 * @langDe ### Beispiel
 *     <p>Basierend auf der INSPIRE-Codelist
 *     [EnvironmentalDomain](https://inspire.ec.europa.eu/codeList/EnvironmentalDomain) werden Werte
 *     wie `soil` auf das deutschsprachige Label in der INSPIRE-Codelist-Registry abgebildet:
 * @langAll <code>
 * ```yaml
 * ---
 * label: Umweltbereich, für den Umweltziele festgelegt werden können.
 * entries:
 *   air: Luft
 *   climateAndClimateChange: Klima und Klimawandel
 *   healthProtection: Gesundheitsschutz
 *   landUse: Bodennutzung
 *   naturalResources: natürliche Ressourcen
 *   natureAndBiodiversity: Natur und biologische Vielfalt
 *   noise: Lärm
 *   soil: Boden
 *   sustainableDevelopment: nachhaltige Entwicklung
 *   waste: Abfall
 *   water: Wasser
 * ```
 * </code>
 * @langEn If the codelist is just an enumeration of values without separate labels, `entries` can
 *     also be a list of the values. Each value is then also its own label:
 * @langDe Ist die Codelist nur eine Aufzählung von Werten ohne eigene Bezeichnungen, dann kann
 *     `entries` auch eine Liste der Werte sein. Jeder Wert ist dann zugleich seine Bezeichnung:
 * @langAll <code>
 * ```yaml
 * ---
 * label: Gebäudefunktion
 * entries:
 *   - Wohngebäude
 *   - Bürogebäude
 *   - Lagerhalle
 * ```
 * </code>
 * @langEn ## Storage
 *     <p>Codelists reside in the [Store](../application/20-configuration/10-store-new.md) as values
 *     with type `codelists`.
 *     <p>Codelists have a path instead of an id. That means for example `values/codelists/bar.yml`
 *     would be referenced as `bar`, but you could additionally define
 *     `values/codelists/foo/bar.yml`. To reference that codelist somewhere else in the
 *     configuration you would need to use `foo/bar`.
 * @langDe ## Speicherung
 *     <p>Die Codelisten liegen im [Store](../application/20-configuration/10-store-new.md) als
 *     Values mit Typ `codelists`.
 *     <p>Codelisten haben einen Pfad anstatt einer Id. Das heißt z.B. `values/codelists/bar.yml`
 *     würde als `bar` referenziert, aber man könnte zusätzlich `values/codelists/foo/bar.yml`
 *     definieren. Um diese Codelist an anderer Stelle in der Konfiguration zu referenzieren würde
 *     man `foo/bar` verwenden.
 * @ref:cfgProperties {@link de.ii.xtraplatform.codelists.domain.ImmutableCodelist}
 */
@DocFile(
    path = "auxiliaries",
    name = "codelists.md",
    tables = {
      @DocTable(
          name = "properties",
          rows = {
            @DocStep(type = Step.TAG_REFS, params = "{@ref:cfgProperties}"),
            @DocStep(type = Step.JSON_PROPERTIES)
          },
          columnSet = ColumnSet.JSON_PROPERTIES)
    })
@Value.Immutable
@Value.Style(
    builder = "new",
    deepImmutablesDetection = true,
    attributeBuilderDetection = true,
    passAnnotations = DocIgnore.class)
@FromValueStore(type = "codelists")
@JsonDeserialize(builder = ImmutableCodelist.Builder.class)
public interface Codelist extends StoredValue {

  enum ImportType {
    TEMPLATES,
    GML_DICTIONARY,
    ONEO_SCHLUESSELLISTE
  }

  abstract class Builder implements ValueBuilder<Codelist> {}

  /**
   * @langEn Human readable label.
   * @langDe Eine lesbare Bezeichnung der Codelist.
   * @default id
   */
  Optional<String> getLabel();

  /**
   * @langEn A description of the codelist.
   * @langDe Eine Beschreibung der Codelist.
   * @default null
   * @since v4.3
   */
  Optional<String> getDescription();

  /**
   * @langEn Map with the original value as key and the new value as value. For a codelist that is
   *     just an enumeration, a list of the values, each value is then mapped to itself.
   * @langDe Jeder Eintrag bildet einen Original-Wert auf den neuen Wert ab. Für eine Codelist, die
   *     nur eine Aufzählung ist, eine Liste der Werte, jeder Wert wird dann auf sich selbst
   *     abgebildet.
   * @default `{}`
   */
  @JsonDeserialize(using = EntriesDeserializer.class)
  Map<String, String> getEntries();

  /**
   * @langEn Always `TEMPLATES`.
   * @langDe Immer `TEMPLATES`.
   * @default TEMPLATES
   */
  Optional<ImportType> getSourceType();

  @DocIgnore
  Optional<String> getSourceUrl();

  /**
   * @langEn Optional default value.
   * @langDe Optional kann ein Defaultwert angegeben werden.
   * @default the value
   */
  Optional<String> getFallback();

  default String getValue(String key) {
    return Optional.ofNullable(getEntries().get(key)).orElse(getFallback().orElse(key));
  }

  /**
   * Reads `entries` either as a map from code to label or, for an enumeration, as a list of codes
   * where each code is also its label.
   */
  class EntriesDeserializer extends StdDeserializer<Map<String, String>> {

    private static final long serialVersionUID = 1L;

    public EntriesDeserializer() {
      super(Map.class);
    }

    @Override
    public Map<String, String> deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      if (!parser.isExpectedStartArrayToken()) {
        return context.readValue(
            parser,
            context
                .getTypeFactory()
                .constructMapType(LinkedHashMap.class, String.class, String.class));
      }

      Map<String, String> entries = new LinkedHashMap<>();
      JsonToken token = parser.nextToken();
      while (token != JsonToken.END_ARRAY) {
        if (token == null || !token.isScalarValue() || token == JsonToken.VALUE_NULL) {
          return context.reportInputMismatch(
              this, "A codelist enumeration must be a list of values, found: %s", token);
        }
        String value = parser.getValueAsString();
        entries.put(value, value);
        token = parser.nextToken();
      }
      return entries;
    }
  }
}
