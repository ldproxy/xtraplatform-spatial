/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocFile;
import de.ii.xtraplatform.values.domain.StoredValue;
import de.ii.xtraplatform.values.domain.ValueBuilder;
import de.ii.xtraplatform.values.domain.annotations.FromValueStore;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * @langEn # Tile Matrix Sets
 *     <p>Custom tiling schemes, in addition to the predefined ones, follow the [OGC Two Dimensional
 *     Tile Matrix Set and Tile Set Metadata 2.0
 *     Standard](https://docs.ogc.org/is/17-083r4/17-083r4.html). They are used by the building
 *     blocks [Tile Matrix Sets](../services/building-blocks/tile_matrix_sets.md) and
 *     [Tiles](../services/building-blocks/tiles.md) and by tile providers.
 *     <p>The JSON representations of the predefined tiling schemes are available on
 *     [GitHub](https://github.com/interactive-instruments/xtraplatform-spatial/tree/master/xtraplatform-tiles/src/main/resources/tilematrixsets)
 *     and can serve as templates.
 * @langDe # Tile Matrix Sets
 *     <p>Benutzerdefinierte Kachelschemas, zusätzlich zu den vordefinierten, folgen dem Standard
 *     [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata
 *     2.0](https://docs.ogc.org/is/17-083r4/17-083r4.html). Sie werden von den Bausteinen [Tile
 *     Matrix Sets](../services/building-blocks/tile_matrix_sets.md) und
 *     [Tiles](../services/building-blocks/tiles.md) sowie von Tile-Providern genutzt.
 *     <p>Die JSON-Darstellungen der vordefinierten Kachelschemas sind auf
 *     [GitHub](https://github.com/interactive-instruments/xtraplatform-spatial/tree/master/xtraplatform-tiles/src/main/resources/tilematrixsets)
 *     verfügbar und können als Vorlage dienen.
 * @langEn ## Storage
 *     <p>Tiling schemes reside in the [Store](../application/20-configuration/10-store-new.md) as
 *     values with type `tile-matrix-sets` and relative path `{tileMatrixSetId}.json`.
 * @langDe ## Speicherung
 *     <p>Kachelschemas liegen im [Store](../application/20-configuration/10-store-new.md) als
 *     Values mit Typ `tile-matrix-sets` und dem relativen Pfad `{tileMatrixSetId}.json`.
 */
@DocFile(path = "auxiliaries", name = "tile-matrix-sets.md")
@Value.Immutable
@Value.Style(deepImmutablesDetection = true, builder = "new")
@FromValueStore(type = "tile-matrix-sets")
@JsonInclude(Include.NON_EMPTY)
@JsonDeserialize(builder = ImmutableTileMatrixSetData.Builder.class)
public interface TileMatrixSetData extends StoredValue {

  String getId();

  Optional<String> getTitle();

  Optional<String> getDescription();

  List<String> getKeywords();

  String getCrs();

  Optional<URI> getWellKnownScaleSet();

  Optional<URI> getUri();

  Optional<TilesBoundingBox> getBoundingBox();

  List<TileMatrix> getTileMatrices();

  List<String> getOrderedAxes();

  abstract class Builder implements ValueBuilder<TileMatrixSetData> {}
}
