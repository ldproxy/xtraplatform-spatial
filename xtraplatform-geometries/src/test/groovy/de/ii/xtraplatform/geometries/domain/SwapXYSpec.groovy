/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain

import de.ii.xtraplatform.geometries.domain.transform.CoordinatesTransformation
import de.ii.xtraplatform.geometries.domain.transform.ImmutableSwapXY
import de.ii.xtraplatform.geometries.domain.transform.SwapXY
import spock.lang.Specification

class SwapXYSpec extends Specification {

    def '2d'() {

        given:

        CoordinatesTransformation next = Mock()
        SwapXY swapXY = ImmutableSwapXY.of(Optional.of(next))
        double[] coordinates = [10.81, 10.33, 10.91, 20.05]
        int dimension = 2

        when:

        swapXY.onCoordinates(coordinates, coordinates.length, dimension, Optional.of(PositionList.Interpolation.LINE), OptionalInt.empty())

        then:

        1 * next.onCoordinates([10.33, 10.81, 20.05, 10.91], 4, 2, Optional.of(PositionList.Interpolation.LINE), OptionalInt.empty())
        0 * _
    }

    def '3d'() {

        given:

        CoordinatesTransformation next = Mock()
        SwapXY swapXY = ImmutableSwapXY.of(Optional.of(next))
        double[] coordinates = [10.81, 10.33, 100.0, 10.91, 20.05, 110.0]
        int dimension = 3

        when:

        swapXY.onCoordinates(coordinates, coordinates.length, dimension, Optional.of(PositionList.Interpolation.LINE), OptionalInt.empty())

        then:

        1 * next.onCoordinates([10.33, 10.81, 100.0, 20.05, 10.91, 110.0], 6, 3, Optional.of(PositionList.Interpolation.LINE), OptionalInt.empty())
        0 * _
    }
}
