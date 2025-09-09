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
import de.ii.xtraplatform.cql.domain.ArrayFunction
import de.ii.xtraplatform.cql.domain.Between
import de.ii.xtraplatform.cql.domain.BinaryArrayOperation
import de.ii.xtraplatform.cql.domain.BinarySpatialOperation
import de.ii.xtraplatform.cql.domain.Bbox
import de.ii.xtraplatform.cql.domain.Casei
import de.ii.xtraplatform.cql.domain.Cql
import de.ii.xtraplatform.cql.domain.CqlPredicate
import de.ii.xtraplatform.cql.domain.Eq
import de.ii.xtraplatform.cql.domain.Function
import de.ii.xtraplatform.cql.domain.Gte
import de.ii.xtraplatform.cql.domain.In
import de.ii.xtraplatform.cql.domain.IsNull
import de.ii.xtraplatform.cql.domain.Like
import de.ii.xtraplatform.cql.domain.Lte
import de.ii.xtraplatform.cql.domain.Neq
import de.ii.xtraplatform.cql.domain.Not
import de.ii.xtraplatform.cql.domain.Or
import de.ii.xtraplatform.cql.domain.Lt
import de.ii.xtraplatform.cql.domain.Gt
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import de.ii.xtraplatform.cql.domain.Spatial
import de.ii.xtraplatform.cql.domain.SpatialLiteral
import de.ii.xtraplatform.cql.domain.SpatialFunction
import de.ii.xtraplatform.cql.domain.Temporal
import de.ii.xtraplatform.cql.domain.TemporalLiteral
import de.ii.xtraplatform.cql.domain.Vector
import de.ii.xtraplatform.cql.infra.CqlIncompatibleTypes
import de.ii.xtraplatform.crs.domain.BoundingBox
import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.geometries.domain.GeometryType
import de.ii.xtraplatform.geometries.domain.Point
import de.ii.xtraplatform.geometries.domain.LineString
import de.ii.xtraplatform.geometries.domain.Polygon
import de.ii.xtraplatform.geometries.domain.MultiLineString
import de.ii.xtraplatform.geometries.domain.MultiPolygon
import spock.lang.Shared
import spock.lang.Specification

class CqlTypeCheckerSpec extends Specification {

    @Shared
    Cql cql

    @Shared
    CqlTypeAndFunctionChecker visitor

    @Shared
    CqlTypeAndFunctionChecker visitor2

    @Shared
    CqlTypeAndFunctionChecker visitor3

    def setupSpec() {
        cql = new CqlImpl()
        def propertyTypes = ImmutableMap.of(
                "road_class", "STRING",
                "count", "LONG",
                "length", "FLOAT",
                "begin", "DATE",
                "end", "DATE",
                "event", "DATE",
                "id", "INTEGER",
                "number", "INTEGER",
                "name", "STRING",
                "street", "STRING"

        )
        def propertyTypes2 = ImmutableMap.of(
                "has_ticket", "BOOLEAN",
                "location_geometry", "GEOMETRY",
                "seats_per_class", "VALUE_ARRAY",
                "special_seats", "OBJECT_ARRAY",
                "end", "DATE",
                "event", "DATE",
                "id", "INTEGER",
                "number", "INTEGER",
                "day", "DATETIME",
                "instant", "INSTANT",


        )
        def propertyTypes3 = ImmutableMap.of(
                "event", "GEOMETRY",
                "location_geometry", "GEOMETRY",
                "seats_per_class", "VALUE_ARRAY",
                "special_seats", "OBJECT_ARRAY",
                "location_geometry2", "GEOMETRY",
                "viewer_class", "VALUE_ARRAY",
        )
        visitor = new CqlTypeAndFunctionChecker(propertyTypes, cql)
        visitor2 = new CqlTypeAndFunctionChecker(propertyTypes2, cql)
        visitor3 = new CqlTypeAndFunctionChecker(propertyTypes3, cql)
    }

    def 'Ignore invalid properties'() {
        when:
        Gt.of(Property.of("road_class"), ScalarLiteral.of("1")).accept(visitor)
        Gt.of(Property.of("count"), ScalarLiteral.of("3")).accept(visitor)

        then:
        noExceptionThrown()
    }

    def 'Test the predicate-checking visitor'() {
        given:
        // run the test on 2 different queries to make sure that old reports are removed

        when:
                Or.of(
                        Lt.of(Property.of("length"), ScalarLiteral.of(1)),
                        Lt.of(Property.of("length"), Property.of("count")),
                        Gt.of(Property.of("road_class"), ScalarLiteral.of(1),)
                ).accept(visitor)

        then:
        thrown CqlIncompatibleTypes

        and:

        when:
        Gt.of(Property.of("road_class"), ScalarLiteral.of("1")).accept(visitor)

        then:
        noExceptionThrown()
    }

    def 'CASEI must be Strings'() {
        given:
        String cqlText = "CASEI(length) IN (CASEI('Οδος'), CASEI('Straße'))"

        when: 'reading text'
        def test = cql.read(cqlText, Cql.Format.TEXT).accept(visitor)

        then:
        thrown CqlIncompatibleTypes
    }

    // these are tested with a different visitor

    def 'temporal'() {
        given:
        String cqlText = "T_INTERSECTS(TIMESTAMP('2011-12-26T20:55:27Z'), INTERVAL('2011-01-01','2011-12-31')) OR " +
                "T_INTERSECTS(INTERVAL('..','2011-12-31'), INTERVAL(begin,end)) OR " +
                "T_BEFORE(event,DATE('2000-01-01'))"

        when: 'reading text'

        def test = cql.read(cqlText, Cql.Format.TEXT).accept(visitor)

        then:

        noExceptionThrown()


    }

    def 'interval of date and timestamp'() {
        given:
        String cqlText = "T_INTERSECTS(TIMESTAMP('2011-12-26T20:55:27Z'), INTERVAL('2011-01-01','2011-12-31T23:59:59Z')) OR " +
                "T_INTERSECTS(INTERVAL('..','2011-12-31'), INTERVAL(begin,end)) OR " +
                "T_BEFORE(event,DATE('2000-01-01'))"

        when: 'reading text'
        def test = cql.read(cqlText, Cql.Format.TEXT).accept(visitor)

        then:
        thrown CqlIncompatibleTypes
    }

    def 'Test Between'() {

        when:

            Between.of(Property.of("count"), ScalarLiteral.of("1"), ScalarLiteral.of("3")).accept(visitor)

        then:

            noExceptionThrown()

        and:

        when:

            Between.of(Property.of(property), ScalarLiteral.of(scalarLiteral1), ScalarLiteral.of(scalarLiteral2)).accept(visitor)

        then:

            thrown exception

        where:

             property | scalarLiteral1 | scalarLiteral2 | exception
             "id"     | "5"            | "2"            | CqlIncompatibleTypes
             "begin"  | "1"            | "2"            | CqlIncompatibleTypes

    }

    def 'Test Interval'() {

        when:

            Temporal.intervalOf(Property.of("begin"), Property.of("end") ).accept(vis1)

        then:

             noExceptionThrown()

        and:

        when:

            Temporal.intervalOf(Property.of(property21), Property.of(property22) ).accept(vis2)

        then:

            thrown CqlIncompatibleTypes

        where:

        property11           | property12 | vis1     | property21   | property22          | vis2
        Property.of("begin") | "end"      | visitor  | "number"     | "id"                | visitor
        Property.of("end")   | "event"    | visitor  | "begin"      | "id"                | visitor
        Property.of("event") | "day"      | visitor2 | "day"        | "location_geometry" | visitor2
        Property.of("day")   | "event"    | visitor2 | "has_ticket" | "event"             | visitor2

    }

    def 'Test isNull'() {

        when:

             IsNull.of(property).accept(visitor)

        then:

            noExceptionThrown()

        where:

            property << [Property.of("end"), "end"]

    }

    def 'Test In'() {

        when:

             In.of(Property.of("end"), Property.of("begin")).accept(visitor)
             In.of(ScalarLiteral.of(10), Property.of("end")).accept(visitor)

        then:

            noExceptionThrown()



    }

    def 'Test Not'() {

        when:

             Not.of(property).accept(visitor)

        then:

            noExceptionThrown()

        where:

            property << [
                    Lt.of(Property.of("number"), Property.of("id")),
                    Gt.of(Property.of("number"), Property.of("id")),
                    In.of(Property.of("end"), Property.of("begin")),
                    Between.of(Property.of("count"), ScalarLiteral.of("1"), ScalarLiteral.of("3")),
                    CqlPredicate.of(Eq.of("swimming_pool", ScalarLiteral.of(true))),]

    }

    def 'Test Like'() {

        when:

             Like.of(property11, property12).accept(visitor)

        then:

            noExceptionThrown()

        when:

            Like.of(property21, property22).accept(visitor)

        then:

            thrown CqlIncompatibleTypes

        where:

            property11 | property12            | property21 | property22
            "street"   | ScalarLiteral.of("9") | "street"   | ScalarLiteral.of(9)
            "street"   | "name"                | "end"      | ScalarLiteral.of("9")
    }

    def 'Test BinarySpatialOperation'() {

        when:

             BinarySpatialOperation.of(SpatialFunction.S_WITHIN, Property.of("event"),  Property.of("location_geometry")).accept(visitor3)
             BinarySpatialOperation.of(SpatialFunction.S_CONTAINS, Property.of("event"),  Property.of("location_geometry")).accept(visitor3)
             BinarySpatialOperation.of(SpatialFunction.S_DISJOINT, Property.of("event"),  Property.of("location_geometry")).accept(visitor3)
             BinarySpatialOperation.of(SpatialFunction.S_EQUALS, Property.of("event"),  Property.of("location_geometry")).accept(visitor3)
             BinarySpatialOperation.of(SpatialFunction.S_INTERSECTS, Property.of("event"),  Property.of("location_geometry")).accept(visitor3)
             BinarySpatialOperation.of(SpatialFunction.S_OVERLAPS, Property.of("event"),  Property.of("location_geometry")).accept(visitor3)
             BinarySpatialOperation.of(SpatialFunction.S_TOUCHES, Property.of("event"),  Property.of("location_geometry")).accept(visitor3)

        then:

            noExceptionThrown()

        and:

        when:

            BinarySpatialOperation.of(SpatialFunction.S_WITHIN, property1, property2).accept(visitor3)
            BinarySpatialOperation.of(SpatialFunction.S_CONTAINS, property1, property2).accept(visitor3)
            BinarySpatialOperation.of(SpatialFunction.S_DISJOINT, property1, property2).accept(visitor3)
            BinarySpatialOperation.of(SpatialFunction.S_EQUALS, property1, property2).accept(visitor3)
            BinarySpatialOperation.of(SpatialFunction.S_INTERSECTS, property1, property2).accept(visitor3)
            BinarySpatialOperation.of(SpatialFunction.S_OVERLAPS, property1, property2).accept(visitor3)
            BinarySpatialOperation.of(SpatialFunction.S_TOUCHES, property1, property2).accept(visitor3)

        then:

            thrown CqlIncompatibleTypes

        where:

            property1                      | property2
            ScalarLiteral.of(2) as Spatial | ScalarLiteral.of(1) as Spatial
            Property.of("seats_per_class") | Property.of("event")
            ScalarLiteral.of(1) as Spatial | Property.of("event")
            Property.of("special_seats")   | Property.of("event")
    }

    def 'Test BinaryArrayOperation'() {

        when:


            BinaryArrayOperation.of(ArrayFunction.A_OVERLAPS, Property.of("seats_per_class"), Property.of("special_seats")).accept(visitor3)
            BinaryArrayOperation.of(ArrayFunction.A_CONTAINS, Property.of("seats_per_class"), Property.of("viewer_class")).accept(visitor3)
            BinaryArrayOperation.of(ArrayFunction.A_EQUALS, Property.of("seats_per_class"), Property.of("special_seats")).accept(visitor3)
            BinaryArrayOperation.of(ArrayFunction.A_OVERLAPS, Property.of("viewer_class"), Property.of("special_seats")).accept(visitor3)

        then:

            noExceptionThrown()

        when:

            BinaryArrayOperation.of(ArrayFunction.A_OVERLAPS, property1, property2).accept(visitor3)
            BinaryArrayOperation.of(ArrayFunction.A_CONTAINS, property1, property2).accept(visitor3)
            BinaryArrayOperation.of(ArrayFunction.A_EQUALS, property1, property2).accept(visitor3)
            BinaryArrayOperation.of(ArrayFunction.A_OVERLAPS, property1, property2).accept(visitor3)

        then:

            thrown CqlIncompatibleTypes

        where:

            property1                        | property2
            ScalarLiteral.of(2) as Vector    | ScalarLiteral.of(1) as Vector
            Property.of("location_geometry") | Property.of("seats_per_class")
            ScalarLiteral.of("1") as Vector  | Property.of("seats_per_class")
            Property.of("event")             | Property.of("")
    }

    def "Test CASEI"(){
        when:

            Casei.of(Property.of("name")).accept(visitor)

        then:

            noExceptionThrown()

        and:

        when:

            Casei.of(property).accept(vis)

        then:

            thrown CqlIncompatibleTypes

        where:

            property                         | vis
            Property.of("location_geometry") | visitor2
            Property.of("length")            | visitor
            Property.of("event")             | visitor3
    }

    def "Test Accenti"(){
        when:

            Accenti.of(Property.of("name")).accept(visitor)

        then:

            noExceptionThrown()

        and:

        when:

            Accenti.of(property).accept(vis)

        then:

            thrown CqlIncompatibleTypes

        where:

            property                         | vis
            Property.of("location_geometry") | visitor2
            Property.of("length")            | visitor
            Property.of("event")             | visitor3
    }

    def "Test SpatialLiteral"(){
        when:

            SpatialLiteral.of("1").accept(visitor)

        then:

             noExceptionThrown()
    }

    def "Test function"(){

        when:

             Eq.ofFunction(Function.of("position", ImmutableList.of()), ScalarLiteral.of("123")).accept(visitor)

        then:

              thrown CqlIncompatibleTypes
    }


    def "Test Neq"(){

        when:

        boolean b = Neq.of(value1, value2).accept(visitor)

        then:

        b == true

        where:

        value1              | value2                | boolValue
        Property.of("name") | Property.of("street") | true
        "test"              | "test"                | false
        "1"                 | ScalarLiteral.of("1") | false
    }

    def "Test Neq with function"(){

        when:

        boolean b = Neq.ofFunction(Function.of("position", ImmutableList.of()), ScalarLiteral.of("test")).accept(visitor)

        then:

        thrown CqlIncompatibleTypes

    }

    def "Test covering Temporal with function"(){

        when:

        In.of(expression).accept(visitor)

        then:

        noExceptionThrown()

        where:

        expression << [Temporal.intervalOf(TemporalLiteral.of(ImmutableList.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z")), TemporalLiteral.of(ImmutableList.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z"))),
                       Temporal.intervalOf(TemporalLiteral.of(ImmutableList.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z")), Property.of("event")),
                       Temporal.intervalOf(Property.of("event"), TemporalLiteral.of(ImmutableList.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z")))]


    }

    def 'Test Temporal throws IllegalStateException'(){
        when:

        In.of(Temporal.intervalOf(TemporalLiteral.of(ImmutableList.of("2017-06-10T07:30:00Z", "2017-06-11T10:30:00Z")), ScalarLiteral.of(true) as Temporal)).accept(visitor)

        then:

        thrown IllegalStateException

    }

    def 'Test Bbox with bounding box'(){
        when:

        SpatialLiteral.of(Bbox.of(BoundingBox.of(2,2,4,4, OgcCrs.CRS84))).accept(visitor)

        then:

        noExceptionThrown()

    }

    def 'Test Point getType'(){

        when:

        def type = Point.of(1 as double, 1 as double, EpsgCrs.of(555)).getType()

        then:

        type == GeometryType.POINT

    }

    def 'Test LineString getType'(){

        when:

        def type = LineString.of(1 as double, 1 as double, 2 as double, 2 as double).getType()

        then:

        type == GeometryType.LINE_STRING

    }

    def 'Test MultiLineString getType'(){

        when:

        def type = MultiLineString.of(List.of(LineString.of(1 as double, 1 as double, 2 as double, 2 as double),
                LineString.of(1 as double, 1 as double, 2 as double, 2 as double))).getType()

        then:

        type == GeometryType.MULTI_LINE_STRING

    }

    def 'Test MultiPolygon getType'(){

        when:

        def type = MultiPolygon.of(List.of(Polygon.of(new double[]{1 as double, 1 as double, 2 as double, 3 as double, 3 as double, 5 as double, 1 as double, 1 as double}))).getType()

        then:

        type == GeometryType.MULTI_POLYGON

    }

    def 'Test Gte'(){

        when:

        boolean b = Gte.of(value1, value2).accept(visitor)

        then:

        b == result

        where:

        value1                | value2                | result

        "name"                | "name"                | true
        "1"                   | ScalarLiteral.of("1") | true
        Property.of("number") | Property.of("id")     | true
    }

    def 'Test Lte'(){

        when:

        boolean b = Lte.of(value1, value2).accept(visitor)

        then:

        b == result

        where:

        value1                                 | value2                | result

        "name"                                 | "name"                | true
        Property.of("number")                  | Property.of("id")     | true
    }

    def 'Test Coordinate 3D'(){

        when:

        SpatialLiteral.of(Point.of(1.00 as double, 2 as double, 3 as double)).accept(visitor)

        then:

        noExceptionThrown()

    }

    def 'Test Point with Crs'(){

        when:

        SpatialLiteral.of(Point.of(1 as double, 1 as double, EpsgCrs.of(555))).accept(visitor)

        then:

        noExceptionThrown()

    }

    def 'Test In with List'(){
        when:

        In.of(ImmutableList.of(ScalarLiteral.of(1))).accept(visitor)
        In.of(ScalarLiteral.of("name"), ImmutableList.of("name")).accept(visitor)

        then:

        noExceptionThrown()
    }

    def 'Test Gt with 2 Property Strings'(){
        when:

        Gt.of(value1, value2).accept(visitor)

        then:

        noExceptionThrown()

        where:

        value1 | value2

        Property.of("number") | Property.of("id")
        "name"                | "name"


    }
    def 'Test Eq'(){
        when:

        Eq.of(Property.of("name"), Property.of("street")).accept(visitor)

        then:

        noExceptionThrown()

    }

    def 'Gt List throws IllegalStateException'(){
        when:

        Gt.of(ImmutableList.of(ScalarLiteral.of(1), ScalarLiteral.of("d"), ScalarLiteral.of(false)))

        then:

        thrown IllegalStateException

    }

    def 'Gt List throws CqlIncompatibleTypes'(){
        when:

        Gt.of(ImmutableList.of(Property.of("number"), ScalarLiteral.of("1"))).accept(visitor)

        then:

        thrown CqlIncompatibleTypes

    }

    def 'Test Gt'(){
        when:

        Gt.of(ImmutableList.of(Property.of("number"), ScalarLiteral.of("1"))).accept(visitor)

        then:

        thrown CqlIncompatibleTypes

    }

    def 'Test Like with Function'(){
        when:

        Like.ofFunction(Function.of("position", ImmutableList.of()), ScalarLiteral.of(1 as double))

        then:

        noExceptionThrown()

    }

    def 'Test IsNUl with Function'(){
        when:

        IsNull.of(Function.of("position", ImmutableList.of()))

        then:

        noExceptionThrown()

    }

    def 'Test Lt with Function'(){
        when:

        Lt.ofFunction(Function.of("position", ImmutableList.of()), ScalarLiteral.of("1"))

        then:

        noExceptionThrown()

    }

    def 'Test Lte with Function'(){
        when:

        Lte.ofFunction(Function.of("position", ImmutableList.of()), ScalarLiteral.of("1"))

        then:

        noExceptionThrown()

    }

    def 'Test Gt with Function'(){
        when:

        Gt.ofFunction(Function.of("position", ImmutableList.of()), ScalarLiteral.of("1"))

        then:

        noExceptionThrown()

    }
}
