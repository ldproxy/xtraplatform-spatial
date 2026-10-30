/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app

import de.ii.xtraplatform.cql.domain.And
import de.ii.xtraplatform.cql.domain.BooleanValue2
import de.ii.xtraplatform.cql.domain.Cql
import de.ii.xtraplatform.cql.domain.Cql2Expression
import de.ii.xtraplatform.cql.domain.Not
import de.ii.xtraplatform.cql.domain.Or
import spock.lang.Shared
import spock.lang.Specification

class CqlAbsentPropertiesMapperSpec extends Specification {

    static final String DATETIME = "T_INTERSECTS(datetime, INTERVAL('2020-01-01T00:00:00Z','2021-01-01T00:00:00Z'))"
    static final String NAME = "name = 'foo'"

    static final Cql2Expression TRUE = BooleanValue2.of(true)
    static final Cql2Expression FALSE = BooleanValue2.of(false)

    @Shared
    Cql cql

    def setupSpec() {
        cql = new CqlImpl()
    }

    Cql2Expression map(String filter) {
        cql.mapAbsentProperties(read(filter), Set.of("datetime"))
    }

    Cql2Expression read(String filter) {
        cql.read(filter, Cql.Format.TEXT)
    }

    def 'Filters without absent properties are unchanged'() {
        expect:
        map("NOT (${NAME})") == read("NOT (${NAME})")
    }

    def 'Predicates on an absent property do not match'() {
        expect:
        map(DATETIME) == FALSE
        map("NOT ${DATETIME}") == FALSE
    }

    def 'IS NULL on an absent property matches'() {
        expect:
        map("datetime IS NULL") == TRUE
        map("datetime IS NOT NULL") == FALSE
    }

    def 'Negations are pushed down to predicates on absent properties'() {
        expect:
        map("NOT (${DATETIME} AND ${NAME})") == Or.of(FALSE, Not.of(read(NAME)))
        map("NOT (${DATETIME} OR ${NAME})") == And.of(FALSE, Not.of(read(NAME)))
        map("NOT (NOT ${DATETIME} AND ${NAME})") == Or.of(FALSE, Not.of(read(NAME)))
    }

    def 'Other branches of a logical operation are kept'() {
        expect:
        map("${DATETIME} OR ${NAME}") == Or.of(FALSE, read(NAME))
        map("datetime IS NULL AND ${NAME}") == And.of(TRUE, read(NAME))
    }
}
