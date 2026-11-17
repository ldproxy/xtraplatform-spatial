/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.domain;

import de.ii.xtraplatform.base.domain.resiliency.Volatile2;
import java.util.Optional;

/**
 * @author zahnen
 */
public interface CrsTransformerFactory extends Volatile2 {

  boolean isSupported(EpsgCrs crs);

  Optional<CrsTransformer> getTransformer(EpsgCrs sourceCrs, EpsgCrs targetCrs);

  Optional<CrsTransformer> getTransformer(EpsgCrs sourceCrs, EpsgCrs targetCrs, boolean force2d);

  /**
   * Returns a transformer that is selected for an area of interest. When several coordinate
   * operations between the two coordinate reference systems are available, the most accurate one
   * whose area of use contains the area of interest is chosen, e.g. a regional grid instead of a
   * national one. Without an area of interest, or with an area that covers the whole world, the
   * selection is the same as for {@link #getTransformer(EpsgCrs, EpsgCrs, boolean)}.
   *
   * @param areaOfInterest the area of interest, a box in any supported coordinate reference system
   */
  Optional<CrsTransformer> getTransformer(
      EpsgCrs sourceCrs, EpsgCrs targetCrs, boolean force2d, Optional<BoundingBox> areaOfInterest);

  /**
   * Returns a view of this factory that selects all transformers for the given area of interest,
   * see {@link #getTransformer(EpsgCrs, EpsgCrs, boolean, Optional)}. All other methods delegate to
   * this factory. If this factory also implements {@link CrsInfo}, so does the view.
   */
  default CrsTransformerFactory withAreaOfInterest(BoundingBox areaOfInterest) {
    return CrsTransformerFactoryForArea.of(this, areaOfInterest);
  }
}
