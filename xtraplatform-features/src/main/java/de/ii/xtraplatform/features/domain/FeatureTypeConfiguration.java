/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.Optional;

/**
 * @author zahnen
 */
public interface FeatureTypeConfiguration {

  /**
   * @langEn Unique identifier, allowed characters are (A-Z, a-z), numbers (0-9), underscore and
   *     hyphen.
   * @langDe Eindeutiger Identifikator, erlaubt sind Buchstaben (A-Z, a-z), Ziffern (0-9), der
   *     Unterstrich ("_") und der Bindestrich ("-").
   */
  String getId();

  /**
   * @langEn Human readable label.
   * @langDe Menschenlesbare Bezeichnung.
   * @default {id}
   */
  String getLabel();

  /**
   * @langEn Human readable description.
   * @langDe Menschenlesbare Beschreibung.
   * @default ""
   */
  Optional<String> getDescription();
}
