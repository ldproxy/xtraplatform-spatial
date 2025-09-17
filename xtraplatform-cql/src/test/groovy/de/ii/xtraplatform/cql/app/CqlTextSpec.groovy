/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app

import com.google.common.collect.ImmutableMap
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry
import de.ii.xtraplatform.blobs.domain.ResourceStore
import de.ii.xtraplatform.cql.domain.Cql
import de.ii.xtraplatform.cql.domain.Cql2Expression
import de.ii.xtraplatform.cql.domain.CqlParseException
import de.ii.xtraplatform.cql.domain.In
import de.ii.xtraplatform.cql.domain.Interval
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.SCrosses
import de.ii.xtraplatform.cql.domain.SIntersects
import de.ii.xtraplatform.cql.domain.SpatialLiteral
import de.ii.xtraplatform.cql.domain.TemporalFunction
import de.ii.xtraplatform.crs.domain.CrsInfo
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.crs.infra.CrsTransformerFactoryProj
import de.ii.xtraplatform.geometries.domain.Axes
import de.ii.xtraplatform.geometries.domain.GeometryCollection
import de.ii.xtraplatform.geometries.domain.MultiLineString
import de.ii.xtraplatform.geometries.domain.Polygon
import de.ii.xtraplatform.geometries.domain.MultiPolygon
import de.ii.xtraplatform.geometries.domain.Point
import de.ii.xtraplatform.geometries.domain.MultiPoint
import de.ii.xtraplatform.geometries.domain.LineString
import de.ii.xtraplatform.geometries.domain.PositionList
import de.ii.xtraplatform.geometries.domain.Position
import de.ii.xtraplatform.proj.domain.ProjLoaderImpl
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Path
import java.util.concurrent.CompletableFuture

class CqlTextSpec extends Specification {

    @Shared
    Cql cql

    @Shared
    ResourceStore resourceStore

    @Shared
    VolatileRegistry volatileRegistry

    def setupSpec() {
        cql = new CqlImpl()
        resourceStore = Stub()
        volatileRegistry = Stub()
        volatileRegistry.onAvailable(*_) >> CompletableFuture.completedFuture(null)
    }

    def 'Floors greater than 5'() {

        given:
        String cqlText = "floors > 5"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_1

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_1, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Taxes less than or equal to 500'() {

        given:
        String cqlText = "taxes <= 500"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_2

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_2, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Owner name contains "Jones"'() {

        given:
        String cqlText = "owner LIKE '% Jones %'"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_3

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_3, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Owner name starts with "Mike"'() {

        given:
        String cqlText = "owner LIKE 'Mike%'"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_4

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_4, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }


    def 'Owner name does not contain "Mike"'() {

        given:
        String cqlText = "owner NOT LIKE '% Mike %'"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_5

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_5, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }


    def 'A swimming pool'() {

        given:
        String cqlText = "swimming_pool = true"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_6

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_6, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }


    def 'More than 5 floors and a swimming pool'() {

        given:
        String cqlText = "floors > 5 AND swimming_pool = true"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_7

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_7, Cql.Format.TEXT)

        then:
        actual2 == cqlText

    }

    def 'A swimming pool and (more than five floors or material is brick)'() {

        given:
        String cqlText = "swimming_pool = true AND (floors > 5 OR material LIKE 'brick%' OR material LIKE '%brick')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_8

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_8, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def '[More than five floors and material is brick] or swimming pool is true'() {

        given:
        String cqlText = "(floors > 5 AND material = 'brick') OR swimming_pool = true"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_9

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_9, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Not under 5 floors or a swimming pool'() {

        given:
        String cqlText = "NOT (floors < 5) OR swimming_pool = true"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_10

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_10, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Owner name starts with "mike" or "Mike" and is less than 4 floors'() {

        given:
        String cqlText = "(owner LIKE 'mike%' OR owner LIKE 'Mike%') AND floors < 4"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_11

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_11, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Built before 2015'() {

        given:
        String cqlText = "T_BEFORE(built, TIMESTAMP('2012-06-05T00:00:00Z'))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_12

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_12, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Built after June 5, 2012'() {

        given:
        String cqlText = "T_AFTER(built, TIMESTAMP('2012-06-05T00:00:00Z'))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_13

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_13, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Updated between 7:30am June 10, 2017 and 10:30am June 11, 2017'() {

        given:
        String cqlText = "T_DURING(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_14

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_14, Cql.Format.TEXT)

        then:
        actual2 == cqlText

    }

    def 'Location in the box between -118,33.8 and -117.9,34 in long/lat (geometry 1)'() {

        given:
        String cqlText = "S_WITHIN(location, BBOX(-118.0,33.8,-117.9,34.0))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_15

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_15, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Location that intersects with geometry'() {

        given:
        String cqlText = "S_INTERSECTS(location, POLYGON((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0)))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_16

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_16, Cql.Format.TEXT)

        then:
        actual2 == cqlText

    }

    def 'VisitMultipoint'() {

        given:

        String cqlText = "S_INTERSECTS(\"bbox\", MULTIPOINT(12.0 55.09,11.0 54.09))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        noExceptionThrown()

        and:

        when:

        MultiPoint multiPoint = MultiPoint.of(List.of(Point.of((double)12.00, (double)55.09), Point.of((double)11.00, (double)54.09)))
        String actual2 = cql.write( SIntersects.of(Property.of("bbox"), SpatialLiteral.of(multiPoint)), Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'VisitMultiLineString'() {

        given:

        String cqlText = "S_INTERSECTS(\"bbox\", MULTILINESTRING((6.0 57.27,11.0 50.27),(6.0 47.27,11.0 50.27)))"

        LineString lineString1 =  LineString.of(PositionList.of(Axes.XY,new double[]{(double)6.00, (double)57.27, (double)11.00, (double)50.27}))
        LineString lineString2 =  LineString.of(PositionList.of(Axes.XY,new double[]{(double)6.00, (double)47.27, (double)11.00, (double)50.27}))

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        noExceptionThrown()

        and:

        when:

        String actual2 = cql.write( SIntersects.of(Property.of("bbox"), SpatialLiteral.of(MultiLineString.of(List.of(lineString1, lineString2)))), Cql.Format.TEXT)

        then:

        actual2 == cqlText

    }

    def 'VisitMultiPolygon'() {

        given:

        String cqlText = "S_INTERSECTS(\"bbox\", MULTIPOLYGON(((6.0 47.27,11.0 50.27,11.0 50.27,6.0 47.27)),((6.0 47.27,11.0 50.27,11.0 50.27,6.0 47.27))))"

        Polygon polygon1 =  Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{6.0,47.27,11.0,50.27,11.0,50.27,6.0,47.27})))
        Polygon polygon2 =  Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{6.0,47.27,11.0,50.27,11.0,50.27,6.0,47.27})))


        when:

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        noExceptionThrown()

        and:

        when:

        String actual2 = cql.write( SIntersects.of(Property.of("bbox"), SpatialLiteral.of(MultiPolygon.of(List.of(polygon1, polygon2)))), Cql.Format.TEXT)

        then:

        actual2 == cqlText

    }

    def 'VisitGeometryCollection'() {

        given:

        String cqlText = "S_INTERSECTS(\"bbox\", GEOMETRYCOLLECTION Z(LINESTRING Z(6.0 47.27 0.0,11.0 50.27 3.0),POINT Z(0.0 0.0 0.0)))"

        LineString lineString =  LineString.of(PositionList.of(Axes.XYZ, new double[]{(double)6.00, (double)47.27, (double)0, (double)11.00, (double)50.27, (double)3}));
        Point point =  Point.of(Position.ofXYZ((double)0, (double)0, (double)0))
        GeometryCollection geometryCollection = GeometryCollection.of(List.of(lineString, point))

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        noExceptionThrown()

        and:

        when:

        String actual2 = cql.write( SIntersects.of(Property.of("bbox"), SpatialLiteral.of(geometryCollection)), Cql.Format.TEXT)

        then:

        actual2 == cqlText

    }

    def 'VisitPoint'() {

        given:

        String cqlText = "S_INTERSECTS(\"bbox\", POINT(5.0 42.27))"

        when:

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        noExceptionThrown()

        and:

        when:

        String actual2 = cql.write( SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Point.of(5.00, 42.27))) , Cql.Format.TEXT)

        then:

        actual2 == cqlText

    }



    def 'Test with "AND" as predicate1'() {

        //TODO no coverage de.ii.xtraplatform.cql.infra.CqlTextVisitor line 118

        when:

        String cqlText = "true AND false"
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        noExceptionThrown()


    }

    def 'More than 5 floors and is within geometry 1 (below)'() {

        given:
        String cqlText = "floors > 5 AND S_WITHIN(geometry, BBOX(-118.0,33.8,-117.9,34.0))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_17

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_17, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Number of floors between 4 and 8'() {
        given:
        String cqlText = "floors BETWEEN 4 AND 8"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_18

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_18, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Owner name is either Mike, John or Tom'() {
        given:
        String cqlText = "owner IN ('Mike','John','Tom')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_19

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_19, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'owner is NULL'() {
        given:
        String cqlText = "owner IS NULL"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_20

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_20, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'owner is not NULL'() {
        given:
        String cqlText = "owner IS NOT NULL"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_21

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_21, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Built before 2015 (only date, no time information)'() {
        given:
        String cqlText = "T_BEFORE(built, DATE('2015-01-01'))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_24

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_24, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Updated between June 10, 2017 and June 11, 2017'() {
        given:
        String cqlText = "T_DURING(updated, INTERVAL('2017-06-10','2017-06-11'))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_25b

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_25b, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Updated between 7:30am June 10, 2017 and open end date'() {
        given:
        String cqlText = "T_DURING(updated, INTERVAL('2017-06-10T07:30:00Z','..'))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_26

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_26, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Updated between open start date and 10:30am June 11, 2017'() {
        given:
        String cqlText = "T_DURING(updated, INTERVAL('..','2017-06-11T10:30:00Z'))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_27

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_27, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Open interval on both ends'() {
        given:
        String cqlText = "T_DURING(updated, INTERVAL('..','..'))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_28

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_28, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Interval with properties on both ends'() {
        given:
        String cqlText = "T_INTERSECTS(event_date, INTERVAL(startDate,endDate))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_TINTERSECTS

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_TINTERSECTS, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Function with no arguments'() {
        given:
        String cqlText = "pos() = 1"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_29

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_29, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Function with multiple arguments'() {
        given:
        String cqlText = "indexOf(names,'Mike') >= 5"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_30

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_30, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Function with a temporal argument'() {
        given:
        String cqlText = "year(TIMESTAMP('2012-06-05T00:00:00Z')) = 2012"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_31

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_31, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Property with a nested filter'() {
        given:
        String cqlText = "filterValues[property = 'd30'].measure > 0.1"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_32

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_32, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Property with two nested filters'() {
        given:
        String cqlText = "filterValues1[property1 = 'd30'].filterValues2[property2 <= 100].measure > 0.1"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_33

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_33, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Find the Landsat scene with identifier "LC82030282019133LGN00"'() {

        given:
        String cqlText = "landsat:scene_id = 'LC82030282019133LGN00'"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_34

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_34, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Evaluate if the value of an array property contains the specified subset of values'() {

        given:
        String cqlText = "A_CONTAINS(layer:ids, ('layers-ca','layers-us'))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_38

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_38, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Both operands are property references'() {

        given:
        String cqlText = "height < floors"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_37

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_37, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Number of floors NOT between 4 and 8'() {
        given:
        String cqlText = "floors NOT BETWEEN 4 AND 8"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_39

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_39, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Owner name is NOT Mike, John, Tom'() {
        given:
        String cqlText = "owner NOT IN ('Mike','John','Tom')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_40

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_40, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Nested regular filter'() {
        given:
        String cqlText = "filterValues[property = 'd30'].measure > 0.1"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_32

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_32, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Array predicate with nested filter'() {
        given:
        String cqlText = "A_CONTAINS(theme[scheme = 'profile'].concept, ('DLKM','Basis-DLM','DLM50'))"
        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)
        then:
        actual == CqlFilterExamples.EXAMPLE_NESTED_WITH_ARRAYS
        and:
        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_NESTED_WITH_ARRAYS, Cql.Format.TEXT)
        then:
        actual2 == cqlText
    }

    def 'Nested filter with a function'() {
        given:
        String cqlText = "filterValues[position() IN (1,3)].measure BETWEEN 1 AND 5"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_NESTED_FUNCTION

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_NESTED_FUNCTION, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'Nested filter with a function and BETWEEN'() {

        given:

        String cqlText = "filterValues[position() BETWEEN 4 AND 8].measure BETWEEN 1 AND 5"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_NESTED_FUNCTION_BETWEEN

        and:


        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_NESTED_FUNCTION_BETWEEN, Cql.Format.TEXT)

        then:

        actual2 == cqlText

    }

    def 'Array predicate with nested INTERVAL'() {

        given:

        String cqlText = "A_CONTAINS(theme[T_INTERSECTS(event, INTERVAL(start_date,end_date))].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_45

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_45, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested LIKE filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[scheme LIKE 'profile'].concept, ('DLKM','Basis-DLM','DLM50'))"
        //A_CONTAINS(theme[INTERVAL(start_date,end_date)].concept, ('DLKM','Basis-DLM','DLM50'))

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_44

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_44, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested IS NULL filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[scheme IS NULL].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_46

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_46, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested CASEI filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[CASEI(schema) IN (CASEI('region'),CASEI('straße'))].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_47

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_47, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested ACCENTI filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[ACCENTI(schema) IN (ACCENTI('region'),ACCENTI('straße'))].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_48

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_48, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested OR filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[(schema = 'schema_1' OR schema = 'schema_2')].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_49

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_49, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested and filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[(length > 5 AND count > 10)].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_50

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_50, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested lt filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[length < 5].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_51

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_51, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested lte filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[length <= 5].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_53

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_53, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested gte filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[length >= 5].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_52

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_52, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested neq filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[length <> 5].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_54

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_54, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested not filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[NOT (length = 5)].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_55

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_55, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested S_TOUCHES filter'() {
        given:

        String cqlText = "A_CONTAINS(theme[S_TOUCHES(event, location_geometry)].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_56

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_56, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested array operation filter and TemporalLiteral'() {
        given:

        String cqlText = "A_CONTAINS(theme[A_OVERLAPS(event, location_geometry)].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_57

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_57, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'IN predicate with a function'() {
        given:
        String cqlText = "position() IN (1,3)"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_IN_WITH_FUNCTION

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_IN_WITH_FUNCTION, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'true'() {
        given:
        String cqlText = "true"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_TRUE

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_TRUE, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'true AND (false OR NOT (false))'() {
        given:
        String cqlText = "true AND (false OR NOT (false))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_BOOLEAN_VALUES

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_BOOLEAN_VALUES, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'keyword as property name'() {
        given:
        String cqlText = "root.\"date\" > DATE('2022-04-17')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_KEYWORD

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_KEYWORD, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'LT with temporal values'() {
        given:
        String cqlText = "built < TIMESTAMP('2012-06-05T00:00:00Z')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_12_alt

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_12_alt, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'LTEQ with temporal values'() {
        given:
        String cqlText = "built <= TIMESTAMP('2012-06-05T00:00:00Z')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_12eq_alt

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_12eq_alt, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'GT with temporal values'() {
        given:
        String cqlText = "built > TIMESTAMP('2012-06-05T00:00:00Z')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_13_alt

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_13_alt, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'GTEQ with temporal values'() {
        given:
        String cqlText = "built >= TIMESTAMP('2012-06-05T00:00:00Z')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_13eq_alt

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_13eq_alt, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'EQ with temporal values'() {
        given:
        String cqlText = "built = TIMESTAMP('2012-06-05T00:00:00Z')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_13A_alt

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_13A_alt, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'NEQ with temporal values'() {
        given:
        String cqlText = "built <> TIMESTAMP('2012-06-05T00:00:00Z')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_13Aneq_alt

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_13Aneq_alt, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'BETWEEN with temporal arguments'() {
        given:
        String cqlText = "updated BETWEEN TIMESTAMP('2017-06-10T07:30:00Z') AND TIMESTAMP('2017-06-11T10:30:00Z')"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        thrown(CqlParseException)
    }

    def 'IN with temporal arguments'() {
        given:
        String cqlText = "updated IN (TIMESTAMP('2017-06-10T07:30:00Z'),TIMESTAMP('2018-06-10T07:30:00Z'),TIMESTAMP('2019-06-10T07:30:00Z'),TIMESTAMP('2020-06-10T07:30:00Z'))"

        when: 'reading text'
        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:
        actual == CqlFilterExamples.EXAMPLE_IN_WITH_TEMPORAL

        and:

        when: 'writing text'
        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_IN_WITH_TEMPORAL, Cql.Format.TEXT)

        then:
        actual2 == cqlText
    }

    def 'LT with temporal value -- interval'() {
        given:
        String cqlText = "built < INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z')"

        when: 'reading text'
        cql.read(cqlText, Cql.Format.TEXT)

        then:
        thrown(CqlParseException)
    }

    def 'BETWEEN with temporal arguments -- intervals'() {
        given:
        String cqlText = "updated BETWEEN INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z') AND INTERVAL('2018-06-10T07:30:00Z','2018-06-11T10:30:00Z')"

        when: 'reading text'
        cql.read(cqlText, Cql.Format.TEXT)

        then:
        thrown(CqlParseException)
    }

    def 'IN with temporal arguments -- intervals'() {
        given:
        String cqlText = "updated IN (INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'),INTERVAL('2018-06-10T07:30:00Z','2018-06-11T10:30:00Z'))"

        when: 'reading text'
        cql.read(cqlText, Cql.Format.TEXT)

        then:
        thrown(CqlParseException)
    }

    def 'Case insensitive string comparison function CASEI'() {
        given:

        String cqlText = "CASEI(road_class) IN (CASEI('Οδος'),CASEI('Straße'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_CASEI

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_CASEI, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Accent insensitive string comparison function ACCENTI'() {
        given:

        String cqlText = "ACCENTI(road_class) IN (ACCENTI('Οδος'),ACCENTI('Straße'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_ACCENTI

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_ACCENTI, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested array operation T_BEFORE and TIMESTAMP'() {
        given:

        String cqlText = "A_CONTAINS(theme[T_BEFORE(built, TIMESTAMP('2012-06-05T00:00:00Z'))].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_58

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_58, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested array operation S_WITHIN'() {
        given:

        String cqlText = "A_CONTAINS(theme[S_WITHIN(location, BBOX(-118.0,33.8,-117.9,34.0))].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_59

        and:


        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_59, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    //TODO does not cover CqlVisitorCopy visit(Property...
    def 'Array predicate with nested array operation Property comparison'() {
        given:

        String cqlText = "A_CONTAINS(theme[road_class = name].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_60

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_60, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Array predicate with nested array operation and Geometry.LineString Geometry.Polygon '() {
        given:

        String cqlText = "A_CONTAINS(theme[S_WITHIN(POINT(1.0 1.0), POLYGON((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0)))].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.TEXT)

        then:

        actual == CqlFilterExamples.EXAMPLE_61

        and:

        when: 'writing text'

        String actual2 = cql.write(CqlFilterExamples.EXAMPLE_61, Cql.Format.TEXT)

        then:

        actual2 == cqlText
    }

    def 'Force CqlParseException in Cql.read()'() {
        given:

        String cqlText = "limit > visitors"

        when: 'reading text'

        Cql2Expression actual = cql.read(cqlText, Cql.Format.JSON)

        then:

        thrown CqlParseException
    }

    def 'Force IllegalStateException in cql.write'() {

        given:

        String cqlText = "A_CONTAINS(theme[S_WITHIN(POINT(1.0 1.0), POLYGON((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0)))].concept, ('DLKM','Basis-DLM','DLM50'))"

        when: 'reading text'

        //TODO no exception thrown
        String s = cql.write(cql.read(cqlText, Cql.Format.TEXT), Cql.Format.JSON)

        then:

        noExceptionThrown()
    }

    def 'test find invalid properties '() {

        given:

        String cqlText = "limit > visitor_count"

        Collection<String> collection = new ArrayList<String>() {
            {
                add("limit")
            }
        }

        List<String> stringList;
        when: 'reading text'

        stringList = cql.findInvalidProperties(cql.read(cqlText, Cql.Format.TEXT), collection)

        then:

        stringList.get(0) == "visitor_count" && stringList.size() == 1

    }

    def 'Test checkTypes'() {

        given:

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
                "street", "STRING")

        String cqlText = "A_CONTAINS(theme[T_INTERSECTS(event, INTERVAL(start_date,end_date))].concept, ('DLKM','Basis-DLM','DLM50'))"

        List<String> stringList;
        when: 'reading text'

        cql.checkTypes(cql.read(cqlText, Cql.Format.TEXT), propertyTypes)

        then:

        noExceptionThrown()
    }

    def 'Test checkCoordinates'() {

        given:

        String cqlText = "S_INTERSECTS(location, POLYGON((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0)))"

        CrsTransformerFactoryProj transformerFactory = new CrsTransformerFactoryProj(new ProjLoaderImpl(Path.of(System.getProperty("java.io.tmpdir"), "proj", "data")), resourceStore, volatileRegistry)

        when: 'reading text'

        cql.checkCoordinates(cql.read(cqlText, Cql.Format.TEXT), (CrsTransformerFactory) transformerFactory, (CrsInfo) transformerFactory, OgcCrs.CRS84, OgcCrs.CRS84)

        then:

        noExceptionThrown()
    }

    def 'Test mapTemporalOperators'() {

        given:

        String cqlText = "T_DURING(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"

        when: 'reading text'

        cql.mapTemporalOperators(cql.read(cqlText, Cql.Format.TEXT), Set.of(Interval))

        then:

        noExceptionThrown()
    }

    def 'Test covering CqlVisitorMapEnvelopes'() {

        given:

        def transformerFactory = Mock(CrsTransformerFactoryProj)

        transformerFactory.getAxisWithWraparound(_) >> OptionalInt.of(wrappedAxis)

        when: 'reading text'

        cql.mapEnvelopes(cql.read(cqlText, Cql.Format.TEXT), (CrsInfo) transformerFactory)

        then:

        noExceptionThrown()

        where:

        cqlText                                                                | wrappedAxis
        "floors > 5 AND S_WITHIN(geometry, BBOX(-118.0,33.8,-117.9,34.0))" | 0
        "floors > 5 AND S_WITHIN(geometry, BBOX(-110,33.8,-117.9,34.0))"   | 0
        "floors > 5 AND S_WITHIN(geometry, BBOX(-118.0,34.8,-117.9,34.0))" | 1

    }

    def 'Spatialliteral not type of Geometry'() {

        given:

        Cql2Expression expression = SCrosses.of(SpatialLiteral.of("visitor_count"), SpatialLiteral.of("limit"))

        CrsTransformerFactoryProj transformerFactory = new CrsTransformerFactoryProj(new ProjLoaderImpl(Path.of(System.getProperty("java.io.tmpdir"), "proj", "data")), resourceStore, volatileRegistry)

        when: 'reading text'

        cql.mapEnvelopes(expression, transformerFactory)

        then:

        noExceptionThrown()
    }
    def 'Test cover mapTemporalOfdperators'() {

        when:
        String actual = cql.write(CqlFilterExamples.EXAMPLE_25x, Cql.Format.TEXT)
        cql.mapTemporalOperators(cql.read(actual, Cql.Format.TEXT), Set.of(TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_STARTEDBY))
        then:

        noExceptionThrown()

    }
    def 'Test cover mapTemporalOperators'() {

        when: 'reading text'

        cql.mapTemporalOperators(cql.read(cqlText, Cql.Format.TEXT), setTemporal)

        then:

        noExceptionThrown()

        where:

        setTemporal                                                                                                                                                                           | cqlText

        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_STARTEDBY)                              | "updated IN (TIMESTAMP('2017-06-10T07:30:00Z'),TIMESTAMP('2018-06-10T07:30:00Z'),TIMESTAMP('2019-06-10T07:30:00Z'),TIMESTAMP('2020-06-10T07:30:00Z'))"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_STARTEDBY)                              | "T_BEFORE(INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'), updated)"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_STARTEDBY)                              | "T_BEFORE(INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'), INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_BEFORE)                                                                                                                                                     | "T_BEFORE(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_BEFORE, TemporalFunction.T_AFTER, TemporalFunction.T_CONTAINS, TemporalFunction.T_DISJOINT, TemporalFunction.T_EQUALS, TemporalFunction.T_FINISHEDBY)       | "T_OVERLAPPEDBY(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS, TemporalFunction.T_STARTEDBY) | "T_STARTS(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_BEFORE)                                                                                                                                                     | "T_AFTER(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_AFTER, TemporalFunction.T_DISJOINT, TemporalFunction.T_EQUALS, TemporalFunction.T_FINISHEDBY)                                                               | "T_BEFORE(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS, TemporalFunction.T_STARTEDBY) | "T_CONTAINS(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS, TemporalFunction.T_STARTEDBY) | "T_EQUALS(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS)                               | "T_STARTEDBY(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS, TemporalFunction.T_STARTEDBY) | "T_OVERLAPS(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS, TemporalFunction.T_STARTEDBY) | "T_FINISHEDBY(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS, TemporalFunction.T_STARTEDBY) | "T_FINISHES(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS)                                                              | "T_INTERSECTS(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS, TemporalFunction.T_STARTEDBY) | "T_DISJOINT(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS, TemporalFunction.T_STARTEDBY)                                                          | "T_MEETS(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_MEETS, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_OVERLAPS)                                                                                        | "T_METBY(updated, INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_STARTEDBY)                              | "T_OVERLAPS(INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'), event_Date)"
        Set.of(TemporalFunction.T_INTERSECTS, TemporalFunction.T_MEETS, TemporalFunction.T_METBY, TemporalFunction.T_OVERLAPPEDBY, TemporalFunction.T_STARTEDBY)                              | "T_OVERLAPS(INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'), INTERVAL('2017-06-10T07:30:00Z','2017-06-11T10:30:00Z'))"

    }

    def 'Test mapTemporalOperators with Interval'() {

        given:

        String cqlText = "T_INTERSECTS(event_date, INTERVAL(startDate,endDate))"

        when: 'reading text'

        cql.mapTemporalOperators(cql.read(cqlText, Cql.Format.TEXT), Set.of(In))

        then:

        noExceptionThrown()
    }

    def 'Test mapTemporalOperators with de.ii.xtraplatform.cql.domain.Interval'() {

        when: 'reading text'

        cql.mapTemporalOperators(de.ii.xtraplatform.cql.app.CqlFilterExamples.EXAMPLE_25z, Set.of(In))

        then:

        noExceptionThrown()
    }

}
