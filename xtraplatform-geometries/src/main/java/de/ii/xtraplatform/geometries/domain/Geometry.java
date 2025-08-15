/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.transform.GeometryVisitor;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;

public interface Geometry<T> {

  @NotNull
  T getValue();

  @NotNull
  GeometryType getType();

  @NotNull
  Axes getAxes();

  Optional<EpsgCrs> getCrs();

  boolean isEmpty();

  default <U> U accept(GeometryVisitor<U> visitor) {
    return visitor.visit(this);
  }

  @Value.Derived
  @Value.Auxiliary
  default boolean hasZ() {
    return getAxes() == Axes.XYZ || getAxes() == Axes.XYZM;
  }

  @Value.Derived
  @Value.Auxiliary
  default boolean hasM() {
    return getAxes() == Axes.XYM || getAxes() == Axes.XYZM;
  }
}
