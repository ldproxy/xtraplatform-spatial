/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import java.util.Optional;

public interface WithFeatureProvider {
  /**
   * @langEn The id of the feature provider. By default the tile provider id without `-3dtiles` is
   *     used.
   * @langDe Die Id des Feature-Providers. Standardmäßig wird die Tile-Provider-Id ohne `-3dtiles`
   *     verwendet.
   * @default null
   * @since v4.6
   */
  Optional<String> getFeatureProvider();
}
