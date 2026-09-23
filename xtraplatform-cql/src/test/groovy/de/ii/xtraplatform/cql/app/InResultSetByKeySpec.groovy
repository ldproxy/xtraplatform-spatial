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
import de.ii.xtraplatform.cql.domain.ImmutableInResultSetByKey
import de.ii.xtraplatform.cql.domain.InResultSetByKey
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import org.skyscreamer.jsonassert.JSONAssert
import spock.lang.Shared
import spock.lang.Specification

class InResultSetByKeySpec extends Specification {

    @Shared
    Cql cql

    def setupSpec() {
        cql = new CqlImpl()
    }

    static InResultSetByKey gemeinde() {
        return InResultSetByKey.of([
                land           : Property.of("gkz_lan"),
                regierungsbezirk: Property.of("gkz_rbz"),
                kreis          : Property.of("gkz_krs"),
                gemeinde       : Property.of("gkz_gem")
        ], "fs_gemeinde")
    }

    def 'cql2-json round-trip'() {

        given:
        String cqlJson = """
            {
                "op": "inResultSetByKey",
                "args": [
                    {
                        "gemeinde": { "property": "gkz_gem" },
                        "kreis": { "property": "gkz_krs" },
                        "land": { "property": "gkz_lan" },
                        "regierungsbezirk": { "property": "gkz_rbz" }
                    },
                    "fs_gemeinde"
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == gemeinde()
        ((InResultSetByKey) actual).getSetName() == "fs_gemeinde"
        ((InResultSetByKey) actual).getKeyNames() == ["gemeinde", "kreis", "land", "regierungsbezirk"]

        and:

        when: 'writing json'
        String actual2 = cql.write(gemeinde(), Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cqlJson, actual2, true)
    }

    def 'the order of the key parts in the request does not matter'() {

        given: 'the same key, written in two different orders'
        String one = """
            {
                "op": "inResultSetByKey",
                "args": [ { "land": { "property": "gkz_lan" }, "kreis": { "property": "gkz_krs" } }, "fs_kreis" ]
            }
        """
        String other = """
            {
                "op": "inResultSetByKey",
                "args": [ { "kreis": { "property": "gkz_krs" }, "land": { "property": "gkz_lan" } }, "fs_kreis" ]
            }
        """

        when:
        Cql2Expression first = cql.read(one, Cql.Format.JSON)
        Cql2Expression second = cql.read(other, Cql.Format.JSON)

        then: 'both are the same predicate, in canonical part order'
        first == second
        ((InResultSetByKey) first).getKeyNames() == ["kreis", "land"]
        ((InResultSetByKey) first).getProperties()*.getName() == ["gkz_krs", "gkz_lan"]
    }

    def 'a key part keeps its property, not its position'() {

        when: 'two parts of the same type are swapped in the request'
        Cql2Expression actual = cql.read("""
            {
                "op": "inResultSetByKey",
                "args": [ { "kreis": { "property": "gkz_krs" }, "land": { "property": "gkz_lan" } }, "fs_kreis" ]
            }
        """, Cql.Format.JSON)

        then: 'each part still refers to its own property'
        ((InResultSetByKey) actual).getKey().get("land") == Property.of("gkz_lan")
        ((InResultSetByKey) actual).getKey().get("kreis") == Property.of("gkz_krs")
    }

    def 'cql2-json in a conjunction'() {

        given:
        String cqlJson = """
            {
                "op": "and",
                "args": [
                    { "op": "inResultSetByKey", "args": [ { "land": { "property": "sll_lan" } }, "fs_land" ] },
                    { "op": "=", "args": [ { "property": "bez" }, "foo" ] }
                ]
            }
        """

        when: 'reading json'
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)

        then:
        actual == And.of(
                InResultSetByKey.of([land: Property.of("sll_lan")], "fs_land"),
                Eq.of(Property.of("bez"), ScalarLiteral.of("foo")))
    }

    def 'there is no cql2-text encoding'() {

        when:
        cql.write(gemeinde(), Cql.Format.TEXT)

        then:
        thrown Exception
    }

    def 'resolved producer context is not part of the json encoding'() {

        given:
        InResultSetByKey resolved = new ImmutableInResultSetByKey.Builder()
                .from(gemeinde())
                .args(gemeinde().getArgs())
                .producerType("ax_flurstueck")
                .producerKey([land: "gmd_lan", regierungsbezirk: "gmd_rbz", kreis: "gmd_krs", gemeinde: "gmd_gmd"])
                .producerFilter(Eq.of(Property.of("bez"), ScalarLiteral.of("foo")))
                .build()

        when:
        String json = cql.write(resolved, Cql.Format.JSON)

        then:
        JSONAssert.assertEquals(cql.write(gemeinde(), Cql.Format.JSON), json, true)
    }

    def 'invalid arguments are rejected'() {

        when: 'the key is not an object'
        cql.read("""{ "op": "inResultSetByKey", "args": [ { "property": "id" }, "s" ] }""", Cql.Format.JSON)

        then:
        thrown Exception

        when: 'a key part is not a property'
        cql.read("""{ "op": "inResultSetByKey", "args": [ { "land": "gkz_lan" }, "s" ] }""", Cql.Format.JSON)

        then:
        thrown Exception

        when: 'the key has no parts'
        cql.read("""{ "op": "inResultSetByKey", "args": [ {}, "s" ] }""", Cql.Format.JSON)

        then:
        thrown Exception

        when: 'the result set is not a string'
        cql.read("""{ "op": "inResultSetByKey", "args": [ { "land": { "property": "gkz_lan" } }, 5 ] }""", Cql.Format.JSON)

        then:
        thrown Exception
    }
}
