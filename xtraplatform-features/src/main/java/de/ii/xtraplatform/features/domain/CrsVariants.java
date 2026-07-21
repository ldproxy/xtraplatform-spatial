/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * @langEn Some feature types store the same logical position in one of several CRSs — including
 *     CRSs that cannot be expressed as the storage CRS of the property (realizations that map to
 *     the same coordinate reference system, or 1D vertical reference systems). Each variant is
 *     stored in its own property; `crsVariants` on the main geometry property declares which
 *     sibling properties hold the variants. All referenced properties must be siblings of the
 *     geometry property (properties of the same object) and are implicitly `internal`.
 * @langDe Manche Objektarten speichern dieselbe logische Position in einem von mehreren
 *     Koordinatenreferenzsystemen — einschließlich Systemen, die nicht als Speicher-CRS der
 *     Eigenschaft ausgedrückt werden können (Realisierungen, die auf dasselbe
 *     Koordinatenreferenzsystem abgebildet werden, oder eindimensionale Höhenreferenzsysteme). Jede
 *     Variante wird in einer eigenen Eigenschaft gespeichert; `crsVariants` an der
 *     Haupt-Geometrieeigenschaft deklariert, welche Nachbareigenschaften die Varianten enthalten.
 *     Alle referenzierten Eigenschaften müssen Nachbareigenschaften der Geometrieeigenschaft sein
 *     (Eigenschaften desselben Objekts) und sind implizit `internal`.
 */
@Value.Immutable
@Value.Style(builder = "new", deepImmutablesDetection = true)
@JsonDeserialize(builder = ImmutableCrsVariants.Builder.class)
public interface CrsVariants {

  /**
   * @langEn A `STRING` property that stores the URI of the reference system of the position,
   *     verbatim as received (for example, the `srsName` of a GML geometry).
   * @langDe Eine `STRING`-Eigenschaft, die die URI des Referenzsystems der Position unverändert
   *     speichert (zum Beispiel den `srsName` einer GML-Geometrie).
   * @default null
   * @since v4.8
   */
  Optional<String> getCrsProperty();

  /**
   * @langEn A `FLOAT` property that stores the single coordinate of a position in a 1D vertical
   *     reference system.
   * @langDe Eine `FLOAT`-Eigenschaft, die die einzelne Koordinate einer Position in einem
   *     eindimensionalen Höhenreferenzsystem speichert.
   * @default null
   * @since v4.8
   */
  Optional<String> getVerticalProperty();

  /**
   * @langEn `GEOMETRY` properties that store 2D/3D positions in a CRS other than the CRS of the
   *     primary geometry property. Each referenced property must declare the CRS it is stored in in
   *     `nativeCrs` and the identifiers of its reference systems in `originalCrsIdentifiers`;
   *     positions are routed to the variant property that lists the identifier.
   * @langDe `GEOMETRY`-Eigenschaften, die 2D/3D-Positionen in einem anderen
   *     Koordinatenreferenzsystem als dem der primären Geometrieeigenschaft speichern. Jede
   *     referenzierte Eigenschaft muss das CRS, in dem sie gespeichert ist, in `nativeCrs` sowie
   *     die Kennungen ihrer Referenzsysteme in `originalCrsIdentifiers` deklarieren; Positionen
   *     werden der Variante zugeordnet, die die Kennung auflistet.
   * @default []
   * @since v4.8
   */
  List<String> getGeometryProperties();
}
