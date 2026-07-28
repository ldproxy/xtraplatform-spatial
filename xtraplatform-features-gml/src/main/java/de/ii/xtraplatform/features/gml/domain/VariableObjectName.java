/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

import java.util.Map;
import org.immutables.value.Value;

/**
 * Reverse counterpart of the encoder's {@code VariableName}. Configured per object type whose GML
 * object element name varies with a discriminator property value: the decoder, when it sees one of
 * the configured wire element names, emits {@link #getProperty()} as the path segment and the
 * mapped source value at that path.
 *
 * <p>The {@link #getMapping()} direction is reversed relative to the encoder: keys are the
 * qualified wire element names ({@code prefix:LocalName} as they appear on the wire after
 * namespace-prefix normalisation), values are the source-side property values to emit.
 */
@Value.Immutable
public interface VariableObjectName {

  /** Schema property name into which the discriminator value is emitted. */
  String getProperty();

  /**
   * Wire qualified element name → source value. The qualified name uses the prefix declared in the
   * decoder's namespace map for the element's namespace URI.
   */
  Map<String, String> getMapping();
}
