/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain

import de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformation
import de.ii.xtraplatform.geometries.domain.transform.ImmutableReverseLine
import de.ii.xtraplatform.geometries.domain.transform.ReverseLine
import spock.lang.Specification

class ReverseLineSpec extends Specification {

    def '2d'() {

        given:

        CoordinatesTransformation next = Mock()
        ReverseLine reverseLine = ImmutableReverseLine.of(Optional.of(next))
        double[] coordinates = [10.0, 10.0, 11.0, 11.0, 12.0, 9.0, 13.0, 10.0]
        int dimension = 2

        when:

        reverseLine.onCoordinates(coordinates, coordinates.length, dimension, Optional.of(PositionList.Interpolation.LINE), OptionalInt.of(2))

        then:

        1 * next.onCoordinates([13.0, 10.0, 12.0, 9.0, 11.0, 11.0, 10.0, 10.0], coordinates.length, dimension, Optional.of(PositionList.Interpolation.LINE), OptionalInt.of(2))
    }

    def '3d'() {

        given:

        CoordinatesTransformation next = Mock()
        ReverseLine reverseLine = ImmutableReverseLine.of(Optional.of(next))
        double[] coordinates = [10.0, 10.0, 10.0, 11.0, 11.0, 10.0, 12.0, 9.0, 10.0, 13.0, 10.0, 10.0]
        int dimension = 3

        when:

        reverseLine.onCoordinates(coordinates, coordinates.length, dimension, Optional.of(PositionList.Interpolation.LINE), OptionalInt.of(2))

        then:

        1 * next.onCoordinates([13.0, 10.0, 10.0, 12.0, 9.0, 10.0, 11.0, 11.0, 10.0, 10.0, 10.0, 10.0], coordinates.length, dimension, Optional.of(PositionList.Interpolation.LINE), OptionalInt.of(2))
    }
}
