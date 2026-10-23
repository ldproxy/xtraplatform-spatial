/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app

import de.ii.xtraplatform.cql.domain.Cql
import de.ii.xtraplatform.cql.domain.CqlParseException
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import spock.lang.Shared
import spock.lang.Specification

class CqlNulCharacterSpec extends Specification {

    @Shared
    Cql cql = new CqlImpl()

    def 'a character string with U+0000 is not a valid literal'() {
        when:
        ScalarLiteral.of("a\u0000b")

        then:
        thrown(CqlParseException)
    }

    def 'a filter with U+0000 in a character string is rejected (#format)'() {
        when:
        cql.read(filter, format)

        then:
        thrown(CqlParseException)

        where:
        format          | filter
        Cql.Format.TEXT | "name = 'a\u0000b'"
        Cql.Format.JSON | '{"op":"=","args":[{"property":"name"},"a\\u0000b"]}'
    }

    def 'other control characters are accepted'() {
        expect:
        ScalarLiteral.of("a\u0001b").getValue() == "a\u0001b"
    }
}
