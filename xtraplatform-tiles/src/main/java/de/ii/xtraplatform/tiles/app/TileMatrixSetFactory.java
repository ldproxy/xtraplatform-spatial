/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import de.ii.xtraplatform.tiles.domain.TileMatrixSetData;
import de.ii.xtraplatform.values.domain.ValueFactoryAuto;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@AutoBind
public class TileMatrixSetFactory extends ValueFactoryAuto {

  @Inject
  protected TileMatrixSetFactory() {
    super(TileMatrixSetData.class);
  }
}
