/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain

import spock.lang.Specification

class CirclesSpec extends Specification {

    def 'expandCircleToClosed produces (P1, P2, P3, antipode(P2), P1)'() {
        // given: circle through (0,0), (1,1), (2,0) has center (1,0); antipode of (1,1) is (1,-1)
        when:
        double[] expanded = Circles.expandCircleToClosed([0d, 0d, 1d, 1d, 2d, 0d] as double[])

        then:
        expanded == [0d, 0d, 1d, 1d, 2d, 0d, 1d, -1d, 0d, 0d] as double[]
    }

    def 'expandCircleToClosed rejects colinear points'() {
        when:
        Circles.expandCircleToClosed([0d, 0d, 1d, 0d, 2d, 0d] as double[])

        then:
        thrown(IllegalArgumentException)
    }

    def 'isFullCircleClosed accepts the canonical 5-point expansion'() {
        expect:
        Circles.isFullCircleClosed([0d, 0d, 1d, 1d, 2d, 0d, 1d, -1d, 0d, 0d] as double[])
    }

    def 'isFullCircleClosed rejects a 5-point CIRCULARSTRING that is closed but is not a circle'() {
        // Two arcs that form a closed shape but whose 4 distinct control points are not on a
        // common circle.
        expect:
        !Circles.isFullCircleClosed([0d, 0d, 1d, 1d, 2d, 0d, 1d, -3d, 0d, 0d] as double[])
    }

    def 'isFullCircleClosed rejects when start != end'() {
        expect:
        !Circles.isFullCircleClosed([0d, 0d, 1d, 1d, 2d, 0d, 1d, -1d, 0d, 0.5d] as double[])
    }

    def 'isFullCircleClosed rejects non-5-position lists'() {
        expect:
        !Circles.isFullCircleClosed([0d, 0d, 1d, 1d, 0d, 0d] as double[])
    }

    def 'circumcenter of an axis-aligned right triangle'() {
        // Right triangle at origin, (2,0), (0,2): hypotenuse midpoint (1,1) is the circumcenter.
        expect:
        Circles.circumcenter(0d, 0d, 2d, 0d, 0d, 2d) == [1d, 1d] as double[]
    }
}
