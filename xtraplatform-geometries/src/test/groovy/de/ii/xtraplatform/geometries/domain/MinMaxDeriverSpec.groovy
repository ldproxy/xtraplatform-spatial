/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain

import de.ii.xtraplatform.geometries.domain.transform.MinMaxDeriver
import spock.lang.Specification

class MinMaxDeriverSpec extends Specification {

    static Polygon polygon(double... coordinates) {
        return Polygon.of(List.of(PositionList.of(Axes.XY, coordinates)))
    }

    def 'positive coordinates'() {

        given:
        def geometry = polygon(7.0d, 50.0d, 7.1d, 50.0d, 7.1d, 50.1d, 7.0d, 50.1d, 7.0d, 50.0d)

        when:
        double[][] minMax = geometry.accept(new MinMaxDeriver())

        then:
        minMax[0] == [7.0d, 50.0d] as double[]
        minMax[1] == [7.1d, 50.1d] as double[]
    }

    def 'coordinates that are all negative on one axis'() {

        given: 'a polygon west of Greenwich, so every x is negative'
        def geometry = polygon(-118.0d, 33.8d, -117.9d, 33.8d, -117.9d, 34.0d, -118.0d, 34.0d, -118.0d, 33.8d)

        when:
        double[][] minMax = geometry.accept(new MinMaxDeriver())

        then: 'the maximum is the largest x, not the seed value'
        minMax[0] == [-118.0d, 33.8d] as double[]
        minMax[1] == [-117.9d, 34.0d] as double[]
    }

    def 'coordinates that are all negative on both axes'() {

        given:
        def geometry = polygon(-70.0d, -33.5d, -69.9d, -33.5d, -69.9d, -33.4d, -70.0d, -33.4d, -70.0d, -33.5d)

        when:
        double[][] minMax = geometry.accept(new MinMaxDeriver())

        then:
        minMax[0] == [-70.0d, -33.5d] as double[]
        minMax[1] == [-69.9d, -33.4d] as double[]
    }

    def 'negative coordinates across the components of a multi geometry'() {

        given:
        def geometry = MultiPolygon.of(List.of(
                polygon(-10.0d, -10.0d, -9.0d, -10.0d, -9.0d, -9.0d, -10.0d, -9.0d, -10.0d, -10.0d),
                polygon(-8.0d, -8.0d, -7.0d, -8.0d, -7.0d, -7.0d, -8.0d, -7.0d, -8.0d, -8.0d)))

        when:
        double[][] minMax = geometry.accept(new MinMaxDeriver())

        then:
        minMax[0] == [-10.0d, -10.0d] as double[]
        minMax[1] == [-7.0d, -7.0d] as double[]
    }

    def 'a single negative point'() {

        given:
        def geometry = Point.of(-118.0d, -33.8d)

        when:
        double[][] minMax = geometry.accept(new MinMaxDeriver())

        then:
        minMax[0] == [-118.0d, -33.8d] as double[]
        minMax[1] == [-118.0d, -33.8d] as double[]
    }

    def 'a negative line string'() {

        given:
        def geometry = LineString.of(PositionList.of(Axes.XY, [-5.0d, -6.0d, -3.0d, -8.0d] as double[]))

        when:
        double[][] minMax = geometry.accept(new MinMaxDeriver())

        then:
        minMax[0] == [-5.0d, -8.0d] as double[]
        minMax[1] == [-3.0d, -6.0d] as double[]
    }
}
