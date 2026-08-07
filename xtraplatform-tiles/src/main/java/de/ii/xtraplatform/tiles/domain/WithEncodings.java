/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import java.util.Map;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface WithEncodings {
  /**
   * @langEn Supported tile encodings. Keys have to be one of `MVT`, `PNG`, `WebP` and `JPEG`,
   *     values are provided as `{{fileExtension}}` in `urlTemplate`.
   * @langDe Unterstützte Tile-Encodings. Keys müssen eins aus `MVT`, `PNG`, `WebP` und `JPEG` sein,
   *     Werte sind als `{{fileExtension}}` in `urlTemplate` verfügbar.
   * @default {}
   * @since v3.4
   */
  Map<TilesFormat, String> getEncodings();
}
