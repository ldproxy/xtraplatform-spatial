/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.crs.domain.BoundingBox
import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.crs.domain.OgcCrs
import spock.lang.Specification

class SpatialExtentSpec extends Specification {

    def "computed extent has no bounding box"() {
        expect:
        new ImmutableSpatialExtent.Builder().computed(true).build().toBoundingBox(OgcCrs.CRS84).isEmpty()
    }

    def "computed may only be true"() {
        when:
        new ImmutableSpatialExtent.Builder().computed(false).build()

        then:
        thrown(IllegalStateException)
    }

    def "computed must not be combined with coordinates"() {
        when:
        new ImmutableSpatialExtent.Builder().xmin(0).ymin(0).xmax(1).ymax(1).computed(true).build()

        then:
        thrown(IllegalStateException)
    }

    def "coordinates become a bounding box in the given CRS"() {
        given:
        def crs = EpsgCrs.of(25832)

        expect:
        new ImmutableSpatialExtent.Builder().xmin(1).ymin(2).xmax(3).ymax(4).build().toBoundingBox(crs) ==
                Optional.of(BoundingBox.of(1, 2, 3, 4, crs))
        new ImmutableSpatialExtent.Builder().xmin(1).ymin(2).zmin(0).xmax(3).ymax(4).zmax(10).build().toBoundingBox(crs) ==
                Optional.of(BoundingBox.of(1, 2, 0, 3, 4, 10, crs))
    }

    def "incomplete coordinates are rejected"() {
        when:
        new ImmutableSpatialExtent.Builder().xmin(1).ymin(2).xmax(3).build()

        then:
        thrown(IllegalStateException)

        when:
        new ImmutableSpatialExtent.Builder().xmin(1).ymin(2).xmax(3).ymax(4).zmin(0).build()

        then:
        thrown(IllegalStateException)
    }
}
