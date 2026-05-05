/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain

import spock.lang.Specification

class CustomFunctionSpec extends Specification {

    def 'rejects function with both expression and expressions'() {
        when:
        new ImmutableCustomFunction.Builder()
                .name('BAD_BOTH')
                .arguments([])
                .returns(['BOOLEAN'])
                .expression('UPPER($value) LIKE $pattern')
                .expressions(['SQL': 'UPPER($value) LIKE $pattern'])
                .build()

        then:
        def ex = thrown(IllegalStateException)
        ex.message.contains("exactly one of 'expression' or 'expressions'")
    }

    def 'rejects function with neither expression nor expressions'() {
        when:
        new ImmutableCustomFunction.Builder()
                .name('BAD_NONE')
                .arguments([])
                .returns(['BOOLEAN'])
                .build()

        then:
        def ex = thrown(IllegalStateException)
        ex.message.contains("exactly one of 'expression' or 'expressions'")
    }

    def 'accepts function with expression only'() {
        when:
        def function = new ImmutableCustomFunction.Builder()
                .name('OK_SINGLE')
                .arguments([])
                .returns(['BOOLEAN'])
                .expression('UPPER($value) LIKE $pattern')
                .build()

        then:
        function.getExpression() == 'UPPER($value) LIKE $pattern'
        function.getExpressions().isEmpty()
    }

    def 'accepts function with expressions only'() {
        when:
        def function = new ImmutableCustomFunction.Builder()
                .name('OK_MAP')
                .arguments([])
                .returns(['BOOLEAN'])
                .expressions(['SQL/PGIS': 'UPPER($value) LIKE $pattern'])
                .build()

        then:
        function.getExpression() == null
        function.getExpressions().get('SQL/PGIS') == 'UPPER($value) LIKE $pattern'
    }
}
