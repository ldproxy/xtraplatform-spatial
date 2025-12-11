/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain

import de.ii.xtraplatform.tiles.domain.TileSubMatrix
import spock.lang.Specification
import spock.lang.Timeout

import java.util.concurrent.TimeUnit

class TileTreeSpec extends Specification {

    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    def 'level 9 tree'() {
        when:
        TileTree result = TileTree.from(TileSubMatrix.of(9, 0, 511, 0, 511), 2)
        TileTree result2 = TileTree.from(TileSubMatrix.of(9, 0, 511, 0, 510), 3)

        then:
        result.getLevel() == 0
        result.getNumberOfSubtrees() == 69904
        result.getNumberOfTiles() == 262144
        result2.getLevel() == 0
        result2.getNumberOfSubtrees() == 266304
        result2.getNumberOfTiles() == 261632
    }

    def 'morton'() {
        when:
        TileTree subtree1 = TileTree.of(8, 3, 230)
        TileTree subtree2 = TileTree.of(4, 14, 4)

        then:
        subtree1.getMortonCurveIndex(2) == 13
        subtree2.getMortonCurveIndex(2) == 4
    }
}
