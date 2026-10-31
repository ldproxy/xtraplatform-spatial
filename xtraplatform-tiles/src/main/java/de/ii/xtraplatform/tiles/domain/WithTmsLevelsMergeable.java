/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface WithTmsLevelsMergeable extends WithTmsLevels {
  @Override
  BuildableMap<MinMax, ImmutableMinMax.Builder> getLevels();
}
