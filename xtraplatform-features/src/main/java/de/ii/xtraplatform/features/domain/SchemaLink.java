/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

/**
 * Internal representation of a property that is mapped to a web link instead of an inline value.
 * The property value is then not included in the feature properties of a response; instead, each
 * format represents the feature property as a link with the configured link relation type — for
 * example, as an entry in the {@code links} array of a GeoJSON feature. On responses for a single
 * feature, the link is also added as an HTTP {@code Link} header.
 *
 * <p>The URI of the link is constructed from the URI template, where the following parameters are
 * replaced when the link is built:
 *
 * <ul>
 *   <li>{@code {{value}}}: the value of the property, percent-encoded;
 *   <li>{@code {{featureUri}}}: the canonical URI of the feature;
 *   <li>{@code {{collectionUri}}}: the URI of the collection;
 *   <li>{@code {{apiUri}}}: the URI of the landing page.
 * </ul>
 *
 * <p>Links differ from feature references ({@code FEATURE_REF} properties): feature references
 * represent relationships to other features and are included in the feature properties, while links
 * are represented outside of the feature properties in the header or {@code links} array of the
 * format.
 */
@Value.Immutable
@Value.Style(builder = "new")
@JsonDeserialize(builder = ImmutableSchemaLink.Builder.class)
public interface SchemaLink {

  String VALUE = "{{value}}";
  String FEATURE_URI = "{{featureUri}}";
  String COLLECTION_URI = "{{collectionUri}}";
  String API_URI = "{{apiUri}}";

  /**
   * The link relation type, see <a href="https://www.rfc-editor.org/rfc/rfc8288.html">RFC 8288</a>,
   * for example a <a
   * href="https://www.iana.org/assignments/link-relations/link-relations.xhtml">registered relation
   * type</a>.
   */
  String getRel();

  /**
   * The template for the URI of the link. The parameters {@code {{value}}}, {@code {{featureUri}}},
   * {@code {{collectionUri}}} and {@code {{apiUri}}} are replaced when the link is built.
   */
  String getUriTemplate();

  static SchemaLink of(String rel, String uriTemplate) {
    return new ImmutableSchemaLink.Builder().rel(rel).uriTemplate(uriTemplate).build();
  }
}
