/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocFile;
import de.ii.xtraplatform.docs.DocStep;
import de.ii.xtraplatform.docs.DocStep.Step;
import de.ii.xtraplatform.docs.DocTable;
import de.ii.xtraplatform.docs.DocTable.ColumnSet;
import org.immutables.value.Value;

/**
 * # Links
 *
 * @langEn A property can be mapped to a web link instead of an inline value. The property value is
 *     then not included in the feature properties of a response; instead, each format represents
 *     the feature property as a link with the configured link relation type — for example, as an
 *     entry in the `links` array of a GeoJSON feature. On responses for a single feature, the link
 *     is also added as an HTTP `Link` header.
 *     <p>The URI of the link is constructed from the URI template, where the following parameters
 *     are replaced when the link is built:
 *     <p><code>
 * - `{{value}}`: the value of the property, percent-encoded;
 * - `{{featureUri}}`: the canonical URI of the feature;
 * - `{{collectionUri}}`: the URI of the collection;
 * - `{{serviceUri}}`: the URI of the landing page.
 * </code>
 *     <p>Links differ from feature references (`FEATURE_REF` properties): feature references
 *     represent relationships to other features and are included in the feature properties, while
 *     links are represented outside of the feature properties in the header or `links` array of the
 *     format.
 *     <p>{@docTable:properties}
 *     <p>Example: a property whose value is an entry in an external register:
 *     <p><code>
 * ```yaml
 * types:
 *   observation:
 *     type: OBJECT
 *     sourcePath: /observation
 *     properties:
 *       observedProperty:
 *         type: STRING
 *         sourcePath: property_code
 *         link:
 *           rel: related
 *           uriTemplate: 'https://example.com/register/{{value}}'
 * ```
 * </code>
 * @langDe Eine Eigenschaft kann statt auf einen Wert in den Feature-Eigenschaften auf einen
 *     Web-Link abgebildet werden. Der Wert der Eigenschaft wird dann nicht in den
 *     Feature-Eigenschaften einer Antwort ausgegeben; stattdessen repräsentiert jedes Format die
 *     Eigenschaft als Link mit der konfigurierten Linkrelation — zum Beispiel als Eintrag im
 *     `links`-Array eines GeoJSON-Features. Bei Antworten für ein einzelnes Feature wird der Link
 *     zusätzlich als HTTP-`Link`-Header gesetzt.
 *     <p>Die URI des Links wird aus dem URI-Template gebildet, wobei die folgenden Parameter beim
 *     Erzeugen des Links ersetzt werden:
 *     <p><code>
 * - `{{value}}`: der Wert der Eigenschaft, prozent-kodiert;
 * - `{{featureUri}}`: die kanonische URI des Features;
 * - `{{collectionUri}}`: die URI der Collection;
 * - `{{serviceUri}}`: die URI der Landing Page.
 * </code>
 *     <p>Links unterscheiden sich von Feature-Referenzen (`FEATURE_REF`-Eigenschaften):
 *     Feature-Referenzen repräsentieren Beziehungen zu anderen Features und sind Teil der
 *     Feature-Eigenschaften, während Links außerhalb der Feature-Eigenschaften im Header bzw.
 *     `links`-Array des Formats repräsentiert werden.
 *     <p>{@docTable:properties}
 *     <p>Beispiel: eine Eigenschaft, deren Wert ein Eintrag in einem externen Register ist:
 *     <p><code>
 * ```yaml
 * types:
 *   observation:
 *     type: OBJECT
 *     sourcePath: /observation
 *     properties:
 *       observedProperty:
 *         type: STRING
 *         sourcePath: property_code
 *         link:
 *           rel: related
 *           uriTemplate: 'https://example.com/register/{{value}}'
 * ```
 * </code>
 * @ref:properties {@link de.ii.xtraplatform.features.domain.ImmutableSchemaLink}
 */
@DocFile(
    path = "providers/details",
    name = "links.md",
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
@JsonDeserialize(builder = ImmutableSchemaLink.Builder.class)
public interface SchemaLink {

  String VALUE = "{{value}}";
  String FEATURE_URI = "{{featureUri}}";
  String COLLECTION_URI = "{{collectionUri}}";
  String SERVICE_URI = "{{serviceUri}}";

  /**
   * @langEn The link relation type, see [RFC 8288](https://www.rfc-editor.org/rfc/rfc8288.html),
   *     for example a [registered relation
   *     type](https://www.iana.org/assignments/link-relations/link-relations.xhtml).
   * @langDe Die Linkrelation, siehe [RFC 8288](https://www.rfc-editor.org/rfc/rfc8288.html), zum
   *     Beispiel eine [registrierte
   *     Linkrelation](https://www.iana.org/assignments/link-relations/link-relations.xhtml).
   */
  String getRel();

  /**
   * @langEn The template for the URI of the link. The parameters {@code {{value}}}, {@code
   *     {{featureUri}}}, {@code {{collectionUri}}} and {@code {{serviceUri}}} are replaced when the
   *     link is built.
   * @langDe Das Template für die URI des Links. Die Parameter {@code {{value}}}, {@code
   *     {{featureUri}}}, {@code {{collectionUri}}} und {@code {{serviceUri}}} werden beim Erzeugen
   *     des Links ersetzt.
   */
  String getUriTemplate();

  static SchemaLink of(String rel, String uriTemplate) {
    return new ImmutableSchemaLink.Builder().rel(rel).uriTemplate(uriTemplate).build();
  }
}
