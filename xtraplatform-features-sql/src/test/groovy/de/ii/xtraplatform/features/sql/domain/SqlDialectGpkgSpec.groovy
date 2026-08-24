/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain

import spock.lang.Shared
import spock.lang.Specification

class SqlDialectGpkgSpec extends Specification {

    @Shared
    SqlDialectGpkg dialect

    def setupSpec() {
        dialect = new SqlDialectGpkg()
    }

    def 'the spatial index predicate names the r-tree and the key column'() {

        when:
        Optional<String> actual = dialect.getSpatialIndexPredicate(
                "pv_pot_dach", "geom", "A", "id", [365204.0d, 5621522.0d] as double[], [365938.0d, 5622652.0d] as double[])

        then:
        actual.get() == 'A."id" IN (SELECT id FROM "rtree_pv_pot_dach_geom"' +
                ' WHERE maxx >= 365204.0 AND minx <= 365938.0 AND maxy >= 5621522.0 AND miny <= 5622652.0)'
    }

    def 'a bounding box without two axes yields no predicate'() {

        when:
        Optional<String> actual = dialect.getSpatialIndexPredicate(
                "t", "geom", "A", "id", [1.0d] as double[], [2.0d] as double[])

        then:
        actual.isEmpty()
    }

    def 'identifiers are quoted, and embedded quotes escaped'() {

        when:
        Optional<String> actual = dialect.getSpatialIndexPredicate(
                'we"ird', "geom", "A", 'k"ey', [1.0d, 2.0d] as double[], [3.0d, 4.0d] as double[])

        then:
        actual.get().startsWith('A."k""ey" IN (SELECT id FROM "rtree_we""ird_geom"')
    }
}
