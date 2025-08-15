/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain

import de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformation
import de.ii.xtraplatform.geometries.domain.transform.ImmutableSimplifyLine
import de.ii.xtraplatform.geometries.domain.transform.SimplifyLine
import spock.lang.Specification

class SimplifyLineSpec extends Specification {

    def '2d'() {

        given:

        CoordinatesTransformation next = Mock()
        SimplifyLine simplifyLine = ImmutableSimplifyLine.of(Optional.of(next), 1.0)
        double[] coordinates = [10.0, 10.0, 11.0, 11.0, 12.0, 9.0, 13.0, 10.0]
        int dimension = 2

        when:

        simplifyLine.onCoordinates(coordinates, coordinates.length, dimension, Optional.of(PositionList.Interpolation.LINE), OptionalInt.empty())

        then:

        1 * next.onCoordinates([10.0, 10.0, 13.0, 10.0], 4, 2, Optional.of(PositionList.Interpolation.LINE), OptionalInt.empty())
        0 * _
    }

    def '3d'() {

        given:

        CoordinatesTransformation next = Mock()
        SimplifyLine simplifyLine = ImmutableSimplifyLine.of(Optional.of(next), 1.0)
        double[] coordinates = [10.0, 10.0, 10.0, 11.0, 11.0, 10.0, 12.0, 9.0, 10.0, 13.0, 10.0, 10.0]
        int dimension = 3

        when:

        simplifyLine.onCoordinates(coordinates, coordinates.length, dimension, Optional.of(PositionList.Interpolation.LINE), OptionalInt.empty())

        then:

        1 * next.onCoordinates([10.0, 10.0, 10.0, 13.0, 10.0, 10.0], 6, 3, Optional.of(PositionList.Interpolation.LINE), OptionalInt.empty())
        0 * _
    }

    def 'no op'() {

        given:

        CoordinatesTransformation next = Mock()
        SimplifyLine simplifyLine = ImmutableSimplifyLine.of(Optional.of(next), 0.99)
        double[] coordinates = [10.0, 10.0, 11.0, 11.0, 12.0, 9.0, 13.0, 10.0]
        int dimension = 2

        when:

        simplifyLine.onCoordinates(coordinates, coordinates.length, dimension, Optional.of(PositionList.Interpolation.LINE), OptionalInt.empty())

        then:

        1 * next.onCoordinates(coordinates, 8, 2, Optional.of(PositionList.Interpolation.LINE), OptionalInt.empty())
        0 * _
    }

    def 'min points'() {

        given:

        CoordinatesTransformation next = Mock()
        SimplifyLine simplifyLine = ImmutableSimplifyLine.of(Optional.of(next), 1.35)
        double[] coordinates = [10.0, 10.0, 11.0, 11.0, 12.0, 9.0, 13.0, 10.0]
        int dimension = 2

        when:

        simplifyLine.onCoordinates(coordinates, coordinates.length, dimension, Optional.of(PositionList.Interpolation.LINE), OptionalInt.of(3))

        then:

        1 * next.onCoordinates([10.0, 10.0, 11.0, 11.0, 12.0, 9.0, 13.0, 10.0], 8, 2, Optional.of(PositionList.Interpolation.LINE), OptionalInt.of(3))
        0 * _
    }
}
