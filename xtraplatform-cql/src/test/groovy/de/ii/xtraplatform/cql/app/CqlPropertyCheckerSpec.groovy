/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app

import de.ii.xtraplatform.cql.domain.Cql
import spock.lang.Shared
import spock.lang.Specification

class CqlPropertyCheckerSpec extends Specification {

    @Shared
    Cql cql

    def setupSpec() {
        cql = new CqlImpl()
    }

    def 'Test the property-checking visitor'() {
        given:
        // run the test on 2 different queries to make sure that old properties are removed
        def allowedProperties = ["doors", "floors"]
        CqlPropertyChecker visitor = new CqlPropertyChecker(allowedProperties)

        when:
        def invalidProperties = CqlFilterExamples.EXAMPLE_10.accept(visitor, true)

        then:
        invalidProperties.size() == 1
        invalidProperties.get(0) == "swimming_pool"

        and:

        when:
        def invalidProperties2 = CqlFilterExamples.EXAMPLE_21.accept(visitor, true)

        then:
        invalidProperties2.size() == 1
        invalidProperties2.get(0) == "owner"
    }

    def 'Test covering visit(BinaryArrayOperation, List) and visit(ArrayLiteral, List)'() {
        given:

        def allowedProperties = ["doors", "theme.concept"]
        CqlPropertyChecker visitor = new CqlPropertyChecker(allowedProperties)

        when:

        def invalidProperties = CqlFilterExamples.EXAMPLE_44.accept(visitor, true)

        then:

        invalidProperties.size() == 0

    }

    def 'Test covering visit(Interval, List) and visit(BinaryTemporalOperation, List)'() {
        given:

        def allowedProperties = ["event_date", "theme.concept"]
        CqlPropertyChecker visitor = new CqlPropertyChecker(allowedProperties)

        when:

        def invalidProperties = CqlFilterExamples.EXAMPLE_TINTERSECTS.accept(visitor, true)

        then:

        invalidProperties.size() == 2
        invalidProperties.get(0) == "startDate"
        invalidProperties.get(1) == "endDate"

    }

    def 'Test covering visit(SpatialLiteral, List)'() {
        given:

        def allowedProperties = ["event_date", "theme.concept"]
        CqlPropertyChecker visitor = new CqlPropertyChecker(allowedProperties)

        when:

        def invalidProperties = CqlFilterExamples.EXAMPLE_17.accept(visitor, true)

        then:

        invalidProperties.size() == 2
        invalidProperties.get(0) == "floors"
        invalidProperties.get(1) == "geometry"

    }

    def 'Test covering visit(Like, List)'() {
        given:

        def allowedProperties = ["event_date", "theme.concept"]
        CqlPropertyChecker visitor = new CqlPropertyChecker(allowedProperties)

        when:

        def invalidProperties = CqlFilterExamples.EXAMPLE_3.accept(visitor, true)

        then:

        invalidProperties.size() == 1
        invalidProperties.get(0) == "owner"

    }

    def 'Test covering visit(Between, List)'() {
        given:

        def allowedProperties = ["event_date", "theme.concept"]
        CqlPropertyChecker visitor = new CqlPropertyChecker(allowedProperties)

        when:

        def invalidProperties = CqlFilterExamples.EXAMPLE_NESTED_FUNCTION.accept(visitor, true)

        then:

        invalidProperties.size() == 1
        invalidProperties.get(0) == "filterValues.measure"

    }

    def 'Test covering visit(In, List)'() {
        given:

        def allowedProperties = ["event_date", "theme.concept"]
        CqlPropertyChecker visitor = new CqlPropertyChecker(allowedProperties)

        when:

        def invalidProperties = CqlFilterExamples.EXAMPLE_LOWER.accept(visitor, true)

        then:

        invalidProperties.size() == 1
        invalidProperties.get(0) == "road_class"

    }

    def 'Test covering visit(Casei, List)'() {
        given:

        def allowedProperties = ["event_date", "theme.concept"]
        CqlPropertyChecker visitor = new CqlPropertyChecker(allowedProperties)

        when:

        def invalidProperties = CqlFilterExamples.EXAMPLE_CASEI.accept(visitor, true)

        then:

        invalidProperties.size() == 1
        invalidProperties.get(0) == "road_class"

    }

    def 'Test covering visit(Accenti, List)'() {

        given:

        def allowedProperties = ["event_date", "theme.concept"]
        CqlPropertyChecker visitor = new CqlPropertyChecker(allowedProperties)

        when:

        def invalidProperties = CqlFilterExamples.EXAMPLE_ACCENTI.accept(visitor, true)

        then:

        invalidProperties.size() == 1
        invalidProperties.get(0) == "road_class"

    }


}
