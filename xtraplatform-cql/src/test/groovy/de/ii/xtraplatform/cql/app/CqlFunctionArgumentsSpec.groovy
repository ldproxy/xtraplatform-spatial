/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app

import de.ii.xtraplatform.cql.domain.Cql
import de.ii.xtraplatform.cql.domain.Cql2Expression
import de.ii.xtraplatform.cql.domain.CqlNode
import de.ii.xtraplatform.cql.domain.CqlVisitorCopy
import de.ii.xtraplatform.cql.domain.Eq
import de.ii.xtraplatform.cql.domain.Function
import de.ii.xtraplatform.cql.domain.Parameter
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import de.ii.xtraplatform.jsonschema.domain.ImmutableJsonSchemaNumber
import spock.lang.Shared
import spock.lang.Specification

class CqlFunctionArgumentsSpec extends Specification {

    @Shared
    Cql cql

    def setupSpec() {
        cql = new CqlImpl()
    }

    def 'numeric arguments of a function in CQL2 JSON are numbers'() {

        given:
        String cqlJson = """
            {
                "op": "=",
                "args": [
                    {"function": {"name": "dwithin", "args": [{"property": "geometrie"}, ${literal}]}},
                    true
                ]
            }
        """

        when:
        Cql2Expression actual = cql.read(cqlJson, Cql.Format.JSON)
        def argument = ((Function) ((Eq) actual).getArgs().get(0)).getArgs().get(1)

        then:
        argument == expected

        where:
        literal || expected
        "300"   || ScalarLiteral.of(300)
        "300.5" || ScalarLiteral.of(300.5d)
    }

    def 'literal arguments of a function keep their type when written to and read from CQL2 JSON'() {

        given:
        def filter = Eq.ofFunction(Function.of("dwithin", [Property.of("geometrie"), literal]), ScalarLiteral.of(true))

        when:
        Cql2Expression actual = cql.read(cql.write(filter, Cql.Format.JSON), Cql.Format.JSON)

        then:
        actual == filter

        where:
        literal << [ScalarLiteral.of(300), ScalarLiteral.of(300.5d), ScalarLiteral.of("300")]
    }

    def 'a copying visitor replaces parameters in the arguments of a function'() {

        given:
        def parameter = Parameter.of("afstand", new ImmutableJsonSchemaNumber.Builder().build())
        def filter = Eq.ofFunction(Function.of("dwithin", [Property.of("geometrie"), parameter]), ScalarLiteral.of(true))
        def resolver = new CqlVisitorCopy() {
            @Override
            CqlNode visit(Parameter p, List<CqlNode> children) {
                return ScalarLiteral.of(300)
            }
        }

        when:
        def actual = filter.accept(resolver)

        then:
        actual == Eq.ofFunction(Function.of("dwithin", [Property.of("geometrie"), ScalarLiteral.of(300)]), ScalarLiteral.of(true))
    }
}
