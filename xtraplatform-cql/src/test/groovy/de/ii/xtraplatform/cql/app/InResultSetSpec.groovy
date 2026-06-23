/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app

import de.ii.xtraplatform.cql.domain.And
import de.ii.xtraplatform.cql.domain.Cql
import de.ii.xtraplatform.cql.domain.Cql2Expression
import de.ii.xtraplatform.cql.domain.Eq
import de.ii.xtraplatform.cql.domain.ImmutableInResultSet
import de.ii.xtraplatform.cql.domain.InResultSet
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import org.skyscreamer.jsonassert.JSONAssert
import spock.lang.Shared
import spock.lang.Specification

class InResultSetSpec extends Specification {

    @Shared
    Cql cql

    def setupSpec() {
        cql = new CqlImpl()
    }

    def 'cql2-json round-trip'() {

        given:
        String cqlJson = """
            {
                "op": "inResultSet",
                "args": [ { "property": "id" }, "flst" ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == InResultSet.of("id", "flst")
        ((InResultSet) actual).getSetName() == "flst"

        and:

        when: 'writing json'
        String actual2 = cql.write(InResultSet.of("id", "flst"), Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'cql2-json in a conjunction'() {

        given:
        String cqlJson = """
            {
                "op": "and",
                "args": [
                    { "op": "inResultSet", "args": [ { "property": "istBestandteilVon" }, "bb" ] },
                    { "op": "=", "args": [ { "property": "name" }, "foo" ] }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == And.of(
                InResultSet.of("istBestandteilVon", "bb"),
                Eq.of(Property.of("name"), ScalarLiteral.of("foo")))
    }

    def 'cql2-text round-trip'() {

        when: 'writing text'
        String text = cql.write(InResultSet.of("id", "flst"), Cql.Format.TEXT)

        then:
        text == "INRESULTSET(id, 'flst')"

        and:

        when: 'reading text'
        Cql2Expression actual = cql.read(text, Cql.Format.TEXT)

        then:
        actual == InResultSet.of("id", "flst")
    }

    def 'resolved producer context is not part of the json encoding'() {

        given:
        InResultSet resolved = new ImmutableInResultSet.Builder()
                .from(InResultSet.of("id", "flst"))
                .producerType("ax_flurstueck")
                .producerFilter(Eq.of(Property.of("name"), ScalarLiteral.of("foo")))
                .build()

        when:
        String json = cql.write(resolved, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals("""{ "op": "inResultSet", "args": [ { "property": "id" }, "flst" ] }""", json, true)
    }

    def 'invalid arguments are rejected'() {

        when: 'the second argument is not a string'
        cql.read("""{ "op": "inResultSet", "args": [ { "property": "id" }, 5 ] }""", Cql.Format.JSON)

        then:
        thrown Exception

        when: 'the first argument is not a property'
        cql.read("""{ "op": "inResultSet", "args": [ "id", "flst" ] }""", Cql.Format.JSON)

        then:
        thrown Exception
    }
}
