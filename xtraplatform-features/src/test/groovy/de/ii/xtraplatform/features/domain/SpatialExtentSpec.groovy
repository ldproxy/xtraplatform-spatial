/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

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
}
