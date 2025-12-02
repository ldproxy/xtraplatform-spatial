/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.immutables.value.Value;

public interface Tile3dGenerationOptions {

  /**
   * @langEn The first level of the tileset which will contain buildings. The value will depend on
   *     the spatial extent of the dataset, i.e., at what level of the implicit tiling scheme large
   *     buildings can be displayed.
   * @langDe Die erste Ebene des Kachelsatzes, die Gebäude enthalten wird. Der Wert hängt von der
   *     räumlichen Ausdehnung des Datensatzes ab, d. h. davon, auf welcher Ebene des impliziten
   *     Kachelschemas große Gebäude dargestellt werden können.
   * @default 0
   * @since v3.4
   */
  @Nullable
  Integer getFirstLevelWithContent();

  /**
   * @langEn The last level of the tileset which will contain buildings. The value will depend on
   *     the spatial extent of the dataset, i.e., at what level of the implicit tiling scheme small
   *     buildings can be displayed in detail.
   * @langDe Die erste Ebene des Kachelsatzes, die Gebäude enthalten wird. Der Wert hängt von der
   *     räumlichen Ausdehnung des Datensatzes ab, d. h. davon, auf welcher Ebene des impliziten
   *     Kachelschemas große Gebäude dargestellt werden können.
   * @default 0
   * @since v3.4
   */
  @Nullable
  Integer getMaxLevel();

  /**
   * @langEn A CQL2 text filter expression for each level between the `firstLevelWithContent` and
   *     the `maxLevel` to select the buildings to include in the tile on that level. Since the
   *     [refinement strategy](https://docs.ogc.org/cs/22-025r4/22-025r4.html#toc19) is always
   *     `ADD`, specify disjoint filter expressions, so that each building will be included on
   *     exactly one level.
   * @langDe Ein CQL2-Text-Filterausdruck für jede Ebene zwischen `firstLevelWithContent` und
   *     `maxLevel` zur Auswahl der Gebäude, die in die Kachel auf dieser Ebene aufgenommen werden
   *     sollen. Da die
   *     [Verfeinerungsstrategie](https://docs.ogc.org/cs/22-025r4/22-025r4.html#toc19) immer `ADD`
   *     ist, geben Sie disjunkte Filterausdrücke an, sodass jedes Gebäude auf genau einer Ebene
   *     einbezogen wird.
   * @default []
   * @since v3.4
   */
  List<String> getContentFilters();

  /**
   * @langEn A CQL2 text filter expression for each level between the `firstLevelWithContent` and
   *     the `maxLevel` to select the buildings to include in the tile on that level or in any of
   *     the child tiles. This filter expression is the same as all the `contentFilters` on this or
   *     higher levels combined with an `OR`. This is also the default value. However, depending on
   *     the filter expressions, this may lead to inefficient tile filters and to improve
   *     performance the tile filters can also be specified explicitly.
   * @langDe Ein CQL2-Text-Filterausdruck für jede Ebene zwischen `firstLevelWithContent` und
   *     `maxLevel` zur Auswahl der Gebäude, die in die Kachel auf dieser Ebene aufgenommen werden
   *     sollen oder in eine Kachel auf den tieferen Ebenen. Dieser Filterausdruck ist derselbe wie
   *     alle `contentFilters` auf dieser oder tieferen Ebenen, kombiniert mit einem `OR`. Dies ist
   *     auch der Standardwert. Je nach den Filterausdrücken kann dies jedoch zu ineffizienten
   *     Kachelfiltern führen, und zur Verbesserung der Leistung können die Kachelfilter auch
   *     explizit angegeben werden.
   * @default [ ... ]
   * @since v3.4
   */
  @Value.Default
  default List<String> getTileFilters() {
    int levels = getContentFilters().size();
    return IntStream.range(0, levels)
        .mapToObj(
            i ->
                String.format(
                    "(%s)",
                    IntStream.range(i, levels)
                        .mapToObj(j -> getContentFilters().get(j))
                        .collect(Collectors.joining(") OR ("))))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * @langEn The error, in meters, introduced if a tile at level 0 (root) is rendered and its
   *     children at level 1 are not. At runtime, the geometric error is used to compute screen
   *     space error (SSE), i.e., the error measured in pixels.
   * @langDe Der Fehler in Metern, der entsteht, wenn eine Kachel auf Ebene 0 (Root) gerendert wird,
   *     ihre Kinder auf Ebene 1 jedoch nicht. Zur Laufzeit wird der geometrische Fehler zur
   *     Berechnung des Bildschirmabstandsfehlers (SSE) verwendet, d. h. des in Pixeln gemessenen
   *     Fehlers.
   * @default 0
   * @since v3.4
   */
  @Nullable
  Float getGeometricErrorRoot();

  /**
   * @langEn The number of levels in each Subtree.
   * @langDe Die Anzahl der Ebenen in jedem Subtree.
   * @default 3
   * @since v3.4
   */
  @Nullable
  Integer getSubtreeLevels();

  /**
   * @langEn If set to `true`, each feature will be translated vertically so that the bottom of the
   *     feature is on the WGS 84 ellipsoid. Use this option, if the data is intended to be rendered
   *     without a terrain model.
   * @langDe Bei der Einstellung `true` wird jedes Feature vertikal so verschoben, dass der Boden
   *     des Features auf dem WGS 84-Ellipsoid liegt. Verwenden Sie diese Option, wenn die Daten
   *     ohne ein Geländemodell gerendert werden sollen.
   * @default false
   * @since v4.6
   */
  @Nullable
  Boolean getClampToEllipsoid();

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean shouldClampToEllipsoid() {
    return Boolean.TRUE.equals(getClampToEllipsoid());
  }
}
