/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import de.ii.xtraplatform.cql.domain.Accenti
import de.ii.xtraplatform.cql.domain.Casei
import de.ii.xtraplatform.cql.domain.Cql
import de.ii.xtraplatform.cql.domain.Function
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import de.ii.xtraplatform.cql.domain.SpatialLiteral
import de.ii.xtraplatform.cql.domain.TemporalLiteral
import de.ii.xtraplatform.cql.infra.CqlIncompatibleTypes
import spock.lang.Shared
import spock.lang.Specification

class CqlFunctionCheckerSpec extends Specification {

    @Shared
    Cql cql

    @Shared
    CqlTypeAndFunctionChecker visitor

    def setupSpec() {
        cql = new CqlImpl()
        def propertyTypes = ImmutableMap.of(
                "string", "STRING",
                "boolean", "BOOLEAN",
                "geometry", "GEOMETRY",
                "value_array", "VALUE_ARRAY",
                "object_array", "OBJECT_ARRAY",
                "date", "DATE",
                "integer", "INTEGER",
                "interval", "INTERVAL",
                "datetime", "DATETIME",
                "instant", "INSTANT"
        )
        visitor = new CqlTypeAndFunctionChecker(propertyTypes, cql)
    }

    def 'UPPER() and LOWER(): valid expressions'() {
        when:
        Function.of("UPPER", ImmutableList.of(ScalarLiteral.of("test"))).accept(visitor)
        Function.of("UPPER", ImmutableList.of(Property.of("string"))).accept(visitor)
        Function.of("UPPER", ImmutableList.of(Accenti.of(Casei.of(Property.of("string"))))).accept(visitor)
        Function.of("LOWER", ImmutableList.of(ScalarLiteral.of("test"))).accept(visitor)
        Function.of("LOWER", ImmutableList.of(Property.of("string"))).accept(visitor)
        Function.of("LOWER", ImmutableList.of(Accenti.of(Casei.of(Property.of("string"))))).accept(visitor)
        Function.of("LOWER", ImmutableList.of(Function.of("UPPER", ImmutableList.of(Property.of("string"))))).accept(visitor)

        then:
        noExceptionThrown()
    }

    def 'UPPER() and LOWER(): type errors'() {
        when:
        Function.of("UPPER", ImmutableList.of(ScalarLiteral.of(123))).accept(visitor)
        Function.of("UPPER", ImmutableList.of(Property.of("integer"))).accept(visitor)
        Function.of("UPPER", ImmutableList.of(Accenti.of(Casei.of(Property.of("integer"))))).accept(visitor)
        Function.of("LOWER", ImmutableList.of(ScalarLiteral.of(123))).accept(visitor)
        Function.of("LOWER", ImmutableList.of(Property.of("geometry"))).accept(visitor)
        Function.of("LOWER", ImmutableList.of(Accenti.of(Casei.of(Property.of("geometry"))))).accept(visitor)
        Function.of("LOWER", ImmutableList.of(Function.of("UPPER", ImmutableList.of(Property.of("date"))))).accept(visitor)

        then:
        thrown CqlIncompatibleTypes
    }

    def 'UPPER() and LOWER(): incorrect argument count'() {
        when:
        Function.of("UPPER", ImmutableList.of()).accept(visitor)
        Function.of("UPPER", ImmutableList.of(ScalarLiteral.of("test"), ScalarLiteral.of("test"))).accept(visitor)
        Function.of("UPPER", ImmutableList.of(ScalarLiteral.of("test"), Property.of("string"))).accept(visitor)

        then:
        thrown IllegalArgumentException
    }

    def 'UPPER() and LOWER(): incorrect CQL2 node type'() {
        when:
        Function.of("UPPER", ImmutableList.of(TemporalLiteral.of("2025-04-12"))).accept(visitor)

        then:
        thrown IllegalArgumentException
    }

    def 'POSITION(): valid expressions'() {
        when:
        Function.of("POSITION", ImmutableList.of()).accept(visitor)

        then:
        noExceptionThrown()
    }

    def 'POSITION(): incorrect argument count'() {
        when:
        Function.of("POSITION", ImmutableList.of(ScalarLiteral.of(1))).accept(visitor)

        then:
        thrown IllegalArgumentException
    }

    def 'DIAMETER2D() and DIAMETER3D(): valid expressions'() {
        when:
        Function.of("DIAMETER2D", ImmutableList.of(Property.of("geometry"))).accept(visitor)
        Function.of("DIAMETER3D", ImmutableList.of(Property.of("geometry"))).accept(visitor)

        then:
        noExceptionThrown()
    }

    def 'DIAMETER2D() and DIAMETER3D(): type errors'() {
        when:
        Function.of("DIAMETER2D", ImmutableList.of(Property.of("integer"))).accept(visitor)
        Function.of("DIAMETER2D", ImmutableList.of(Function.of("UPPER", ImmutableList.of(Property.of("geometry"))))).accept(visitor)
        Function.of("DIAMETER2D", ImmutableList.of(Function.of("UPPER", ImmutableList.of(Property.of("string"))))).accept(visitor)
        Function.of("DIAMETER3D", ImmutableList.of(Property.of("integer"))).accept(visitor)
        Function.of("DIAMETER3D", ImmutableList.of(Function.of("UPPER", ImmutableList.of(Property.of("geometry"))))).accept(visitor)
        Function.of("DIAMETER3D", ImmutableList.of(Function.of("UPPER", ImmutableList.of(Property.of("string"))))).accept(visitor)

        then:
        thrown CqlIncompatibleTypes
    }

    def 'DIAMETER2D() and DIAMETER3D(): incorrect argument count'() {
        when:
        Function.of("DIAMETER2D", ImmutableList.of()).accept(visitor)
        Function.of("DIAMETER2D", ImmutableList.of(Property.of("geometry"), Property.of("geometry"))).accept(visitor)
        Function.of("DIAMETER3D", ImmutableList.of()).accept(visitor)
        Function.of("DIAMETER3D", ImmutableList.of(Property.of("geometry"), Property.of("geometry"))).accept(visitor)

        then:
        thrown IllegalArgumentException
    }

    def 'DIAMETER2D() and DIAMETER3D(): incorrect CQL2 node type'() {
        when:
        Function.of("DIAMETER2D", ImmutableList.of(SpatialLiteral.of("POINT(0 0"))).accept(visitor)
        Function.of("DIAMETER3D", ImmutableList.of(SpatialLiteral.of("POINT(0 0"))).accept(visitor)

        then:
        thrown IllegalArgumentException
    }

    def 'ALIKE(): valid expressions'() {
        when:
        Function.of("ALIKE", ImmutableList.of(Property.of("value_array"), ScalarLiteral.of("A%"))).accept(visitor)

        then:
        noExceptionThrown()
    }

    def 'ALIKE(): type errors'() {
        when:
        Function.of("ALIKE", ImmutableList.of(Property.of("string"), ScalarLiteral.of("A%"))).accept(visitor)
        Function.of("ALIKE", ImmutableList.of(Property.of("value_array"), ScalarLiteral.of(123))).accept(visitor)
        Function.of("ALIKE", ImmutableList.of(Property.of("value_array"), ScalarLiteral.of(true))).accept(visitor)

        then:
        thrown CqlIncompatibleTypes
    }

    def 'ALIKE(): incorrect argument count'() {
        when:
        Function.of("ALIKE", ImmutableList.of()).accept(visitor)
        Function.of("ALIKE", ImmutableList.of(Property.of("value_array"), ScalarLiteral.of("A%"), ScalarLiteral.of("B%"))).accept(visitor)

        then:
        thrown IllegalArgumentException
    }

    def 'ALIKE(): incorrect CQL2 node type'() {
        when:
        Function.of("ALIKE", ImmutableList.of(Property.of("value_array"), SpatialLiteral.of("POINT(0 0"))).accept(visitor)
        Function.of("ALIKE", ImmutableList.of(SpatialLiteral.of("POINT(0 0"), ScalarLiteral.of("A%"))).accept(visitor)

        then:
        thrown IllegalArgumentException
    }
}
