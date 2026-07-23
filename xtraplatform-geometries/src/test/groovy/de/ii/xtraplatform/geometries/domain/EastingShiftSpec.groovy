/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain

import de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformer
import de.ii.xtraplatform.geometries.domain.transform.ImmutableEastingShift
import spock.lang.Specification

class EastingShiftSpec extends Specification {

    def 'adds the difference to the easting only'() {
        given:
        // zone-prefix-less Gauss-Krueger easting (false easting 500000) shifted to the
        // EPSG:5677 form (false easting 3500000)
        def point = Point.of(446104.620d, 5551059.770d)

        when:
        def shifted = point.accept(new CoordinatesTransformer(
                ImmutableEastingShift.of(Optional.empty(), 3000000d))) as Point

        then:
        shifted.getValue().getCoordinates() == [3446104.62d, 5551059.77d] as double[]
    }

    def 'a negative difference reproduces the wire form without floating-point noise'() {
        given:
        // 3446104.62 - 3000000 in plain double arithmetic yields 446104.6200000001
        def point = Point.of(3446104.62d, 5551059.77d)

        when:
        def shifted = point.accept(new CoordinatesTransformer(
                ImmutableEastingShift.of(Optional.empty(), -3000000d))) as Point

        then:
        shifted.getValue().getCoordinates() == [446104.62d, 5551059.77d] as double[]
    }

    def 'z ordinates are untouched for 3D positions'() {
        given:
        def point = Point.of(446104.62d, 5551059.77d, 123.456d)

        when:
        def shifted = point.accept(new CoordinatesTransformer(
                ImmutableEastingShift.of(Optional.empty(), 3000000d))) as Point

        then:
        shifted.getValue().getCoordinates() == [3446104.62d, 5551059.77d, 123.456d] as double[]
    }
}
