/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.Optional;
import org.immutables.value.Value;

/**
 * A feature property captured for representation as a web link instead of an inline value, see
 * {@link SchemaLink}. Captured by {@code FeatureTokenTransformerPropertyLinks} from properties with
 * an {@link FeatureSchema#getEffectiveLink() effective link}; the URI template is resolved against
 * the request URIs in the API layer, which knows the service, collection and feature URIs.
 */
@Value.Immutable
public interface PropertyLink {

  /** The link relation type. */
  String getRel();

  /** The URI template with unresolved parameters, see {@link SchemaLink#getUriTemplate()}. */
  String getUriTemplate();

  /** The property value; DATETIME values are normalized to ISO 8601 instants. */
  String getValue();

  /** The label of the property, if configured; used as the link title. */
  Optional<String> getTitle();

  static PropertyLink of(String rel, String uriTemplate, String value, Optional<String> title) {
    return ImmutablePropertyLink.builder()
        .rel(rel)
        .uriTemplate(uriTemplate)
        .value(value)
        .title(title)
        .build();
  }
}
