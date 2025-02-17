/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.crs.domain.EpsgCrs;

public interface FeatureCrs {

  String CAPABILITY = "crs";
  String LABEL = "Retrieve CRS information";

  EpsgCrs getNativeCrs();

  // TODO: is there a way to move the whole crs transformation stuff to the provider?
  // as the crs is part of the query, crs transformation should be part of the normalization
  boolean isCrsSupported(EpsgCrs crs);

  default boolean is3dSupported() {
    return false;
  }
}
