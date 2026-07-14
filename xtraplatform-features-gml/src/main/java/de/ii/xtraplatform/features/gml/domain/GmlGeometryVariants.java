/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Routing of a geometry property's positions to CRS-specific variant properties, keyed by the
 * verbatim {@code srsName} on the wire. Some application schemas (AAA/NAS {@code AX_PunktortAU})
 * carry exactly one position per feature, but in one of several CRSs — including realizations that
 * map to the same EPSG code (so the resolved CRS cannot reproduce the original srsName) and 1D
 * vertical systems (which have no representation in the geometry model). Each variant is stored in
 * its own schema property; the verbatim srsName is stored alongside so the encoder can reproduce
 * it.
 *
 * <p>All property values name sibling properties of the geometry property this instance is
 * configured for (same parent object, usually the feature type itself).
 */
@Value.Immutable
public interface GmlGeometryVariants {

  /**
   * Wire {@code srsName} → geometry property for 2D/3D positions in a non-native CRS. The decoder
   * routes the decoded geometry to the mapped property instead of the base geometry property; the
   * resolved CRS (via {@code srsNameMappings}) tags the geometry for the storage transformation.
   */
  Map<String, String> getBySrsName();

  /**
   * Wire {@code srsName} → scalar (FLOAT) property for 1D positions. The single coordinate value is
   * emitted verbatim at the mapped property; no geometry is built.
   */
  Map<String, String> getVerticalBySrsName();

  /**
   * STRING property that receives the verbatim wire {@code srsName} whenever a position was routed
   * via {@link #getBySrsName()} or {@link #getVerticalBySrsName()}. Unset for positions in the
   * native CRS.
   */
  Optional<String> getSrsNameProperty();
}
