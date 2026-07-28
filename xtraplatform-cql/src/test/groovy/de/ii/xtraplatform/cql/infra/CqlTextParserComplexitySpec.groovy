/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.infra

import de.ii.xtraplatform.cql.domain.CqlParseException
import de.ii.xtraplatform.crs.domain.OgcCrs
import spock.lang.Specification

class CqlTextParserComplexitySpec extends Specification {

    CqlTextParser parser = new CqlTextParser()

    def "a deeply nested filter is rejected with CqlParseException, not a StackOverflowError"() {
        given:
        String cql = ("(" * 300) + "foo = 1" + (")" * 300)

        when:
        parser.parse(cql, OgcCrs.CRS84)

        then:
        CqlParseException e = thrown()
        e.message.toLowerCase().contains("nested too deeply")
    }

    def "an over-long filter is rejected"() {
        given:
        String cql = "a" * 100_001

        when:
        parser.parse(cql, OgcCrs.CRS84)

        then:
        CqlParseException e = thrown()
        e.message.toLowerCase().contains("maximum length")
    }

    def "parentheses inside a string literal do not count toward the nesting limit"() {
        given:
        String cql = "foo = '" + ("(" * 300) + "'"

        when:
        parser.parse(cql, OgcCrs.CRS84)

        then:
        noExceptionThrown()
    }

    def "a normal filter parses"() {
        when:
        parser.parse("foo = 'bar'", OgcCrs.CRS84)

        then:
        noExceptionThrown()
    }
}
