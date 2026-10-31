/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app

import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry
import de.ii.xtraplatform.crs.domain.BoundingBox
import de.ii.xtraplatform.crs.domain.CrsTransformationException
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.ImmutableSpatialExtent
import de.ii.xtraplatform.features.domain.SpatialExtent
import de.ii.xtraplatform.tiles.domain.ImmutableMinMax
import de.ii.xtraplatform.tiles.domain.ImmutableTileProviderHttpData
import de.ii.xtraplatform.tiles.domain.ImmutableTilesetHttp
import de.ii.xtraplatform.tiles.domain.ImmutableTilesetHttpDefaults
import de.ii.xtraplatform.tiles.domain.TileMatrixSet
import de.ii.xtraplatform.tiles.domain.TileMatrixSetRepository
import de.ii.xtraplatform.tiles.domain.TilesFormat
import de.ii.xtraplatform.tiles.domain.TilesetHttp
import spock.lang.Specification

class TileProviderHttpExtentSpec extends Specification {

    static SpatialExtent TILESET_EXTENT = new ImmutableSpatialExtent.Builder().xmin(7).ymin(50).xmax(8).ymax(51).build()
    static SpatialExtent DEFAULTS_EXTENT = new ImmutableSpatialExtent.Builder().xmin(5).ymin(47).xmax(15).ymax(55).build()
    static SpatialExtent COMPUTED = new ImmutableSpatialExtent.Builder().computed(true).build()

    TileMatrixSetRepository tileMatrixSets = Stub()
    CrsTransformerFactory crsTransformerFactory = Stub()

    TileMatrixSet tileMatrixSet(String id, BoundingBox crs84) {
        TileMatrixSet tms = Stub()
        tms.getId() >> id
        tms.getBoundingBoxCrs84(_) >> crs84
        tileMatrixSets.get(id) >> Optional.of(tms)
        tms
    }

    TileProviderHttp provider(SpatialExtent defaultsExtent) {
        def defaults = new ImmutableTilesetHttpDefaults.Builder()
                .putLevels("Unknown", new ImmutableMinMax.Builder().min(0).max(10))
                .putEncodings(TilesFormat.MVT, "pbf")
        if (defaultsExtent != null) {
            defaults.extent(defaultsExtent)
        }
        def data = new ImmutableTileProviderHttpData.Builder()
                .id("tiles")
                .providerType("TILE")
                .providerSubType("HTTP")
                .tilesetDefaults(defaults.build())
                .build()
        new TileProviderHttp(tileMatrixSets, crsTransformerFactory, Stub(VolatileRegistry), data)
    }

    static TilesetHttp tileset(SpatialExtent extent, List<String> tileMatrixSetIds = []) {
        def builder = new ImmutableTilesetHttp.Builder().id("ts").urlTemplate("https://example.com/{z}/{y}/{x}")
        tileMatrixSetIds.each { builder.putLevels(it, new ImmutableMinMax.Builder().min(0).max(10)) }
        if (extent != null) {
            builder.extent(extent)
        }
        builder.build()
    }

    static BoundingBox crs84(double xmin, double ymin, double xmax, double ymax) {
        BoundingBox.of(xmin, ymin, xmax, ymax, OgcCrs.CRS84)
    }

    def "the tileset extent wins over the defaults"() {
        expect:
        provider(DEFAULTS_EXTENT).loadMetadata(tileset(TILESET_EXTENT)).bounds == Optional.of(crs84(7, 50, 8, 51))
    }

    def "the defaults extent is used if the tileset has none"() {
        expect:
        provider(DEFAULTS_EXTENT).loadMetadata(tileset(null)).bounds == Optional.of(crs84(5, 47, 15, 55))
    }

    def "without a fixed extent the union of the tile matrix set extents is used"() {
        given:
        tileMatrixSet("A", crs84(0, 0, 10, 10))
        tileMatrixSet("B", crs84(-5, 5, 5, 20))

        expect:
        provider(null).loadMetadata(tileset(null, ["A", "B"])).bounds == Optional.of(crs84(-5, 0, 10, 20))
    }

    def "computed on the tileset discards the defaults extent"() {
        given:
        tileMatrixSet("A", crs84(0, 0, 10, 10))

        expect:
        provider(DEFAULTS_EXTENT).loadMetadata(tileset(COMPUTED, ["A"])).bounds == Optional.of(crs84(0, 0, 10, 10))
    }

    def "tile matrix sets that cannot be transformed are skipped"() {
        given:
        TileMatrixSet broken = Stub()
        broken.getBoundingBoxCrs84(_) >> { throw new CrsTransformationException("no") }
        tileMatrixSets.get("broken") >> Optional.of(broken)

        expect:
        provider(null).loadMetadata(tileset(null, ["broken"])).bounds.isEmpty()
    }
}
