/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

public interface TileGenerationOptions {

  /**
   * @langEn Maximum number of features contained in a single tile per query. This option is ignored
   *     in the optimized PostGIS MVT tile generation.
   * @langDe Steuert die maximale Anzahl der Features, die pro Query für eine Kachel berücksichtigt
   *     werden. Diese Option wird bei der optimierten PostGIS MVT-Kachelgenerierung ignoriert.
   * @default 100000
   * @since v3.4
   */
  @Nullable
  Integer getFeatureLimit();

  /**
   * @langEn Features with line geometries shorter that the given value are excluded from tiles.
   *     Features with surface geometries smaller than the square of the given value are excluded
   *     from the tiles. The value `0.5` corresponds to half a "pixel" in the used coordinate
   *     reference system. This option is ignored in the optimized PostGIS MVT tile generation.
   * @langDe Objekte mit Liniengeometrien, die kürzer als der Wert sind, werden nicht in die Kachel
   *     aufgenommen. Objekte mit Flächengeometrien, die kleiner als das Quadrat des Werts sind,
   *     werden nicht in die Kachel aufgenommen. Der Wert 0.5 entspricht einem halben "Pixel" im
   *     Kachelkoordinatensystem. Diese Option wird bei der optimierten PostGIS
   *     MVT-Kachelgenerierung ignoriert.
   * @default 0.5
   * @since v3.4
   */
  @Nullable
  Double getMinimumSizeInPixel();

  /**
   * @langEn Ignore features with invalid geometries. Before ignoring a feature, an attempt is made
   *     to transform the geometry to a valid geometry. The topology of geometries might be invalid
   *     in the data source or in some cases the quantization of coordinates to integers might
   *     render it invalid. This option is ignored in the optimized PostGIS MVT tile generation.
   * @langDe Steuert, ob Objekte mit ungültigen Objektgeometrien ignoriert werden. Bevor Objekte
   *     ignoriert werden, wird zuerst versucht, die Geometrie in eine gültige Geometrie zu
   *     transformieren. Nur wenn dies nicht gelingt, wird die Geometrie ignoriert. Die Topologie
   *     von Geometrien können entweder schon im Provider ungültig sein oder die Geometrie kann in
   *     seltenen Fällen als Folge der Quantisierung der Koordinaten zu Integern für die Speicherung
   *     in der Kachel ungültig werden. Diese Option wird bei der optimierten PostGIS
   *     MVT-Kachelgenerierung ignoriert.
   * @default false
   * @since v3.4
   */
  @Nullable
  Boolean getIgnoreInvalidGeometries();

  /**
   * @langEn The feature type of the tileset is likely sparsely populated and may have a significant
   *     number of tiles without features.
   * @langDe Die dem Tileset zugrundeliegende Objektart ist wahrscheinlich spärlich belegt. Eine
   *     beträchtliche Anzahl von Kacheln könnte keine Features beinhalten.
   * @default false
   * @since v4.2
   */
  @Nullable
  Boolean getSparse();

  /**
   * @langEn Transform the selected features for a certain zoom level. Supported transformations
   *     are: selecting a subset of feature properties (`properties`), spatial merging of features
   *     that intersect (`merge`), with the option to restrict the operations to features with
   *     matching attributes (`groupBy`). See the example below. For `merge`, the resulting object
   *     will only obtain properties that are identical for all merged features. `merge` and
   *     `groupBy` are ignored in the optimized PostGIS MVT tile generation.
   * @langDe Über Transformationen können die selektierten Features in Abhängigkeit der Zoomstufe
   *     nachbearbeitet werden. Unterstützt wird eine Reduzierung der Attribute (`properties`), das
   *     geometrische Verschmelzen von Features, die sich geometrisch schneiden (`merge`), ggf.
   *     eingeschränkt auf Features mit bestimmten identischen Attributen (`groupBy`). Siehe das
   *     Beispiel unten. Beim Verschmelzen werden alle Attribute in das neue Objekt übernommen, die
   *     in den verschmolzenen Features identisch sind. `merge` und `groupBy` wird bei der
   *     optimierten PostGIS MVT-Kachelgenerierung ignoriert.
   * @default {}
   * @since v3.4
   */
  Map<String, List<LevelTransformation>> getTransformations();

  /**
   * @langEn The feature profiles to be applied when generating features for the tileset. Profiles
   *     are ignored in the optimized PostGIS MVT tile generation.
   * @langDe Die Feature-Profile, die bei der Erstellung von Features für das Tileset angewendet
   *     werden sollen. Profile werden bei der optimierten PostGIS MVT-Kachelgenerierung ignoriert.
   * @default []
   * @since v4.3
   */
  List<String> getProfiles();

  /**
   * @langEn The width and height of a tile in the internal coordinate system that is used for the
   *     coordinates in a vector tile. A higher value increases the geometric precision of the tiles
   *     and reduces the simplification of the geometries, but it also increases the size of the
   *     tiles. The value should be a power of 2. Renderers typically restrict the internal
   *     precision, MapLibre for example normalizes all coordinates to a value of `8192`, so higher
   *     values have no effect in such clients. Changing the value invalidates the content of
   *     existing tile caches, the tiles have to be seeded again.
   * @langDe Die Breite und Höhe einer Kachel in dem internen Koordinatensystem, das für die
   *     Koordinaten in einer Vector Tile verwendet wird. Ein höherer Wert erhöht die geometrische
   *     Genauigkeit der Kacheln und reduziert die Vereinfachung der Geometrien, erhöht aber auch
   *     die Größe der Kacheln. Der Wert sollte eine Zweierpotenz sein. Renderer beschränken die
   *     interne Genauigkeit üblicherweise, MapLibre normalisiert zum Beispiel alle Koordinaten auf
   *     einen Wert von `8192`, höhere Werte haben in solchen Clients daher keinen Effekt. Eine
   *     Änderung des Werts invalidiert den Inhalt bestehender Tile-Caches, die Kacheln müssen
   *     erneut geseedet werden.
   * @default 4096
   * @since v4.9
   */
  @Nullable
  Integer getTileExtent();

  default int getTileExtentOrDefault(TileMatrixSetBase tileMatrixSet) {
    return Objects.isNull(getTileExtent()) ? tileMatrixSet.getTileExtent() : getTileExtent();
  }
}
