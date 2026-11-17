/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.infra

import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry
import de.ii.xtraplatform.blobs.domain.ResourceStore
import de.ii.xtraplatform.crs.domain.BoundingBox
import de.ii.xtraplatform.crs.domain.CrsInfo
import de.ii.xtraplatform.crs.domain.CrsTransformer
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory
import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.proj.domain.ProjLoaderImpl
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Path
import java.util.concurrent.CompletableFuture

/**
 * The PROJ data directory of the tests contains no grid files; the only grid is a cropped copy of
 * HeTA2010 (DHDN to ETRS89 for Hessen) in the custom PROJ directory (test resources, `proj`).
 * Without an area of interest, PROJ ranks a gridless Helmert transformation for the whole zone
 * first; for an area inside Hessen, HeTA2010 is the most accurate operation.
 */
class CrsTransformerAreaOfInterestSpec extends Specification {

    static final EpsgCrs GK3 = EpsgCrs.of(5677)
    static final EpsgCrs UTM32 = EpsgCrs.of(25832)

    // Frankfurt am Main in EPSG:5677, and in EPSG:25832 transformed with the full HeTA2010 grid
    static final double[] FRANKFURT_GK3 = [3477335.33, 5552790.87]
    static final double[] FRANKFURT_UTM32_HETA2010 = [477269.5063, 5551009.5743]

    // EPSG area of use of DHDN to ETRS89 (11), the operation with HeTA2010
    static final BoundingBox HESSEN = BoundingBox.of(7.77, 49.39, 10.24, 51.66, OgcCrs.CRS84)

    static final BoundingBox WORLD = BoundingBox.of(-180, -90, 180, 90, OgcCrs.CRS84)

    @Shared
    CrsTransformerFactoryProj transformerFactory

    def setupSpec() {
        Path gridDirectory = Path.of(getClass().getResource("/proj/de_hvbg_hessen_HeTA2010.tif").toURI()).getParent()
        ResourceStore projStore = Stub()
        projStore.asLocalPath(*_) >> Optional.of(gridDirectory)
        ResourceStore resourceStore = Stub()
        resourceStore.with(*_) >> projStore
        VolatileRegistry volatileRegistry = Stub()
        volatileRegistry.onAvailable(*_) >> CompletableFuture.completedFuture(null)
        transformerFactory = new CrsTransformerFactoryProj(new ProjLoaderImpl(Path.of(System.getProperty("java.io.tmpdir"), "proj", "data")), resourceStore, volatileRegistry)
        transformerFactory.onStart(false).toCompletableFuture().join()
    }

    static double distanceToHeTA2010(CrsTransformer transformer) {
        def result = transformer.transform(FRANKFURT_GK3[0], FRANKFURT_GK3[1])
        return Math.hypot(result.getX() - FRANKFURT_UTM32_HETA2010[0], result.getY() - FRANKFURT_UTM32_HETA2010[1])
    }

    def 'without an area of interest, HeTA2010 is not selected'() {
        when:
        CrsTransformer transformer = transformerFactory.getTransformer(GK3, UTM32).get()

        then:
        distanceToHeTA2010(transformer) > 0.1
    }

    def 'an area of interest inside Hessen selects HeTA2010 - #name'() {
        when:
        CrsTransformer transformer = transformerFactory.getTransformer(GK3, UTM32, false, Optional.of(area)).get()

        then:
        distanceToHeTA2010(transformer) < 0.001

        where:
        name                      | area
        "area of use"             | HESSEN
        "Frankfurt"               | BoundingBox.of(8.6, 50.1, 8.7, 50.2, OgcCrs.CRS84)
        "Frankfurt in EPSG:25832" | BoundingBox.of(470000, 5550000, 480000, 5560000, UTM32)
    }

    def 'an area of interest beyond the area of use does not select HeTA2010 - #name'() {
        when:
        CrsTransformer transformer = transformerFactory.getTransformer(GK3, UTM32, false, Optional.of(area)).get()

        then:
        distanceToHeTA2010(transformer) > 0.1

        where:
        name                    | area
        "0.02 degrees larger"   | BoundingBox.of(7.70, 49.37, 10.26, 51.68, OgcCrs.CRS84)
        "crossing the east"     | BoundingBox.of(8.0, 50.0, 10.6, 51.0, OgcCrs.CRS84)
        "whole world"           | WORLD
    }

    def 'the inverse transformation uses HeTA2010 for an area of interest inside Hessen'() {
        when:
        CrsTransformer transformer = transformerFactory.getTransformer(UTM32, GK3, false, Optional.of(HESSEN)).get()
        def result = transformer.transform(FRANKFURT_UTM32_HETA2010[0], FRANKFURT_UTM32_HETA2010[1])

        then:
        Math.hypot(result.getX() - FRANKFURT_GK3[0], result.getY() - FRANKFURT_GK3[1]) < 0.001
    }

    def 'a whole-world area of interest shares the transformer of no area of interest'() {
        when:
        CrsTransformer world = transformerFactory.getTransformer(GK3, UTM32, false, Optional.of(WORLD)).get()
        CrsTransformer none = transformerFactory.getTransformer(GK3, UTM32).get()

        then:
        world.is(none)
    }

    def 'transformers are cached per area of interest'() {
        when:
        CrsTransformer first = transformerFactory.getTransformer(GK3, UTM32, false, Optional.of(HESSEN)).get()
        CrsTransformer second = transformerFactory.getTransformer(GK3, UTM32, false, Optional.of(HESSEN)).get()
        CrsTransformer none = transformerFactory.getTransformer(GK3, UTM32).get()

        then:
        first.is(second)
        !first.is(none)
    }

    def 'a factory view for an area of interest selects HeTA2010 and delegates everything else'() {
        given:
        CrsTransformerFactory view = transformerFactory.withAreaOfInterest(HESSEN)

        when:
        CrsTransformer transformer = view.getTransformer(GK3, UTM32).get()

        then:
        transformer.is(transformerFactory.getTransformer(GK3, UTM32, false, Optional.of(HESSEN)).get())
        distanceToHeTA2010(transformer) < 0.001
        view instanceof CrsInfo
        ((CrsInfo) view).is3d(GK3) == transformerFactory.is3d(GK3)
        view.isSupported(GK3)
        view.getUniqueKey() == transformerFactory.getUniqueKey()
        view.getState() == transformerFactory.getState()
    }

    def 'an explicit area of interest takes precedence over the area of a factory view'() {
        given:
        CrsTransformerFactory view = transformerFactory.withAreaOfInterest(HESSEN)

        when:
        CrsTransformer transformer = view.getTransformer(GK3, UTM32, false, Optional.of(WORLD)).get()

        then:
        transformer.is(transformerFactory.getTransformer(GK3, UTM32).get())
    }
}
