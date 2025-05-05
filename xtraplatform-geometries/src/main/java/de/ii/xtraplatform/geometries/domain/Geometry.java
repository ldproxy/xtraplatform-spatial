/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;

public interface Geometry<T> {

  SimpleFeatureGeometry getType();

  List<T> getCoordinates();

  @Value.Default
  default EpsgCrs getCrs() {
    return OgcCrs.CRS84;
  }

  @Value.Default
  default int getDimension() {
    return 2;
  }

  default String toWkt(Optional<CrsTransformer> crsTransformer, EpsgCrs crs) {
    return "";
  }

  interface ModifiableGeometry<T> {

    ModifiableGeometry<T> setCrs(EpsgCrs crs);

    default ModifiableGeometry<T> setCrs(Optional<EpsgCrs> crs) {
      crs.ifPresent(this::setCrs);
      return this;
    }

    ModifiableGeometry<T> setDimension(int dimension);

    default ModifiableGeometry<T> setDimension(OptionalInt dimension) {
      dimension.ifPresent(this::setDimension);
      return this;
    }

    @Value.Default
    default int getDepth() {
      return 0;
    }

    ModifiableGeometry<T> setDepth(int depth);

    void openChild();

    void closeChild();

    void addCoordinate(T coordinate);
  }
}
