/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry
import de.ii.xtraplatform.blobs.domain.ResourceStore
import de.ii.xtraplatform.cql.app.CqlFilterExamples
import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.Not
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.crs.infra.CrsTransformerFactoryProj
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.proj.domain.ProjLoaderImpl
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Path
import java.util.concurrent.CompletableFuture

class FilterEncoderSqlSpec extends Specification {

    @Shared
    FilterEncoderSql filterEncoder

    @Shared
    FilterEncoderSql filterEncoder2

    def setupSpec() {

        filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, new CqlImpl(), null, false)
        ResourceStore resourceStore = Stub()
        VolatileRegistry volatileRegistry = Stub()
        volatileRegistry.onAvailable(*_) >> CompletableFuture.completedFuture(null)
        CrsTransformerFactoryProj transformerFactory = new CrsTransformerFactoryProj(new ProjLoaderImpl(Path.of(System.getProperty("java.io.tmpdir"), "proj", "data")), resourceStore, volatileRegistry)
        transformerFactory.onStart(false).toCompletableFuture().join()
        filterEncoder2 = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), (CrsTransformerFactory) transformerFactory, null, new CqlImpl(), null, false)

    }

    def 'spatial operation, bbox, no join'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_15

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE ST_Within(AA.location, ST_GeomFromText('POLYGON((-118.0 33.8,-117.9 33.8,-117.9 34.0,-118.0 34.0,-118.0 33.8))',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'negated spatial operation, bbox, no join'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = Not.of(CqlFilterExamples.EXAMPLE_15)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE NOT ST_Within(AA.location, ST_GeomFromText('POLYGON((-118.0 33.8,-117.9 33.8,-117.9 34.0,-118.0 34.0,-118.0 33.8))',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'spatial operation, polygon, 1:n join'() {

        given:
        def instanceContainer = QuerySchemaFixtures.JOINED_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_16

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA JOIN geometry AB ON (AA.id=AB.id) WHERE ST_Intersects(AB.location, ST_GeomFromText('POLYGON((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0))',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'negated spatial operation, polygon, 1:n join'() {

        given:
        def instanceContainer = QuerySchemaFixtures.JOINED_GEOMETRY
        def filter = Not.of(CqlFilterExamples.EXAMPLE_16)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA JOIN geometry AB ON (AA.id=AB.id) WHERE NOT ST_Intersects(AB.location, ST_GeomFromText('POLYGON((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0))',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'spatial operation, bbox, no join with transformed coordinates'() {

        given:

        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_15_RandomCrs

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE ST_Within(AA.location, ST_GeomFromText('POLYGON((33.8 -118.0,33.8 -117.9,34.0 -117.9,34.0 -118.0,33.8 -118.0))',4326)))"

        String actual = filterEncoder2.encode(filter, instanceContainer)

        then:

        actual == expected
    }

    def 'temporal operation, timestamp'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_TIMESTAMP
        def filter = CqlFilterExamples.EXAMPLE_12

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE AA.built::timestamp(0) < TIMESTAMP '2012-06-05T00:00:00Z')"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'negated temporal operation, timestamp'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_TIMESTAMP
        def filter = Not.of(CqlFilterExamples.EXAMPLE_12)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE AA.built::timestamp(0) >= TIMESTAMP '2012-06-05T00:00:00Z')"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'temporal operation, date'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_DATE
        def filter = CqlFilterExamples.EXAMPLE_12_date

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE AA.built::date < DATE '2012-06-05')"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'negated temporal operation, date'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_DATE
        def filter = Not.of(CqlFilterExamples.EXAMPLE_12_date)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE AA.built::date >= DATE '2012-06-05')"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'temporal operation, interval'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = CqlFilterExamples.EXAMPLE_14

        when:
        String expected = "(A.id IN (SELECT AA.id FROM building AA WHERE AA.updated::timestamp(0) > TIMESTAMP '2017-06-10T07:30:00Z') AND A.id IN (SELECT AA.id FROM building AA WHERE AA.updated::timestamp(0) < TIMESTAMP '2017-06-11T10:30:00Z'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'temporal operation, interval 2'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = CqlFilterExamples.EXAMPLE_14_B

        when:
        String expected = "(A.id IN (SELECT AA.id FROM building AA WHERE TIMESTAMP '2017-06-10T07:30:00Z' > AA.updated::timestamp(0)) AND A.id IN (SELECT AA.id FROM building AA WHERE TIMESTAMP '2017-06-11T10:30:00Z' < AA.updated::timestamp(0)))"
        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'temporal operation, interval 3'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = CqlFilterExamples.EXAMPLE_Interval

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE (TIMESTAMP '2017-06-10T07:30:00Z', TIMESTAMP '2017-06-11T10:30:00Z') OVERLAPS (TIMESTAMP '2012-06-05T00:00:00Z', COALESCE(AA.end,TIMESTAMP 'infinity')))"
        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'temporal operation, interval 4'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = CqlFilterExamples.EXAMPLE_Illegal_Interval

        when:
        String expected = "(A.id IN (SELECT AA.id FROM building AA WHERE TIMESTAMP '2017-06-10T07:30:00Z' > AA.updated::timestamp(0)) AND A.id IN (SELECT AA.id FROM building AA WHERE TIMESTAMP '2017-06-11T10:30:00Z' < AA.updated::timestamp(0)))"
        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        thrown(IllegalStateException)

    }

    def 'not operation'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = CqlFilterExamples.EXAMPLE_14_Negation

        when:
        String expected = "(A.id IN (SELECT AA.id FROM building AA WHERE AA.updated::timestamp(0) <= TIMESTAMP '2017-06-10T07:30:00Z') OR A.id IN (SELECT AA.id FROM building AA WHERE AA.updated::timestamp(0) >= TIMESTAMP '2017-06-11T10:30:00Z'))"
        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'is Null test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = CqlFilterExamples.EXAMPLE_20

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE AA.owner IS NULL)"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'not is Null test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = Not.of(CqlFilterExamples.EXAMPLE_20)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE AA.owner IS NOT NULL)"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'not in list test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = CqlFilterExamples.EXAMPLE_40

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE AA.owner NOT IN ('Mike','John','Tom'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'like test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = CqlFilterExamples.EXAMPLE_3

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE AA.owner::varchar LIKE '% Jones %')"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'not like test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = Not.of(CqlFilterExamples.EXAMPLE_3)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE AA.owner::varchar NOT LIKE '% Jones %')"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'casei test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = CqlFilterExamples.EXAMPLE_CASEI

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE LOWER(AA.road_class) IN ('οδος','straße'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'not casei test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = Not.of(CqlFilterExamples.EXAMPLE_CASEI)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE LOWER(AA.road_class) NOT IN ('οδος','straße'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'accenti test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = CqlFilterExamples.EXAMPLE_ACCENTI

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE LOWER(AA.road_class) IN ('οδος','straße'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        thrown(IllegalArgumentException)

    }

    def 'not accenti test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_INTERVAL
        def filter = Not.of(CqlFilterExamples.EXAMPLE_ACCENTI)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE LOWER(AA.road_class) NOT IN ('οδος','straße'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        thrown(IllegalArgumentException)

    }

    def 'polygon test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_16

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE ST_Intersects(AA.location, ST_GeomFromText('POLYGON((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0))',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'negated polygon test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = Not.of(CqlFilterExamples.EXAMPLE_16)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE NOT ST_Intersects(AA.location, ST_GeomFromText('POLYGON((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0))',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'test AContains'() {

        given:
        def instanceContainer = QuerySchemaFixtures.JOINED_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_AContains_ValidFor_JOINED_GEOMETRY

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA JOIN geometry AB ON (AA.id=AB.id) WHERE AB.location IN ('id','location') GROUP BY AA.id HAVING count(distinct AB.location) = 2)"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'test AContains with not'() {

        given:
        def instanceContainer = QuerySchemaFixtures.JOINED_GEOMETRY
        def filter = Not.of(CqlFilterExamples.EXAMPLE_AContains_ValidFor_JOINED_GEOMETRY)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA JOIN geometry AB ON (AA.id=AB.id) WHERE NOT AB.location IN ('id','location') GROUP BY AA.id HAVING count(distinct AB.location) = 2)"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'test AEquals'() {

        given:
        def instanceContainer = QuerySchemaFixtures.JOINED_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_AEquals_ValidFor_JOINED_GEOMETRY

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA JOIN geometry AB ON (AA.id=AB.id) WHERE AB.location IS NOT NULL GROUP BY AA.id HAVING count(distinct AB.location) = 2 AND count(case when AB.location not in ('id','location') then AB.location else null end) = 0)"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'test AOverlaps'() {

        given:
        def instanceContainer = QuerySchemaFixtures.JOINED_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_AOverlaps_ValidFor_JOINED_GEOMETRY

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA JOIN geometry AB ON (AA.id=AB.id) WHERE AB.location IN ('id','location') GROUP BY AA.id)"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'test AContainedBy'() {

        given:
        def instanceContainer = QuerySchemaFixtures.JOINED_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_AContainedBy_ValidFor_JOINED_GEOMETRY

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA JOIN geometry AB ON (AA.id=AB.id) WHERE AB.location IS NOT NULL GROUP BY AA.id HAVING count(case when AB.location not in ('id','location') then AB.location else null end) = 0)"
        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'interval list test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_25z

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE (COALESCE(AA.start,TIMESTAMP '-infinity'), TIMESTAMP 'infinity') OVERLAPS (DATE '2017-06-10',TIMESTAMP 'infinity'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'interval test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_DATE
        def filter = CqlFilterExamples.EXAMPLE_25y

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE (COALESCE(AA.start,TIMESTAMP '-infinity'), COALESCE(AA.end,TIMESTAMP 'infinity')) OVERLAPS (DATE '2017-06-10',DATE '2017-06-12'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'interval test with not'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_DATE
        def filter = Not.of(CqlFilterExamples.EXAMPLE_25y)

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE NOT (COALESCE(AA.start,TIMESTAMP '-infinity'), COALESCE(AA.end,TIMESTAMP 'infinity')) OVERLAPS (DATE '2017-06-10',DATE '2017-06-12'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'multiPolygon test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_16_MultiPolygon

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE ST_Intersects(AA.location, ST_GeomFromText('MULTIPOLYGON(((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0)),((-15.0 -15.0,15.0 -15.0,15.0 15.0,-15.0 -15.0)))',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'multiLineString test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_16_MultiLineString

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE ST_Intersects(AA.location, ST_GeomFromText('MULTILINESTRING((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0),(-15.0 -15.0,15.0 -15.0,15.0 15.0,-15.0 -15.0))',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'lineString test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_16_LineString

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE ST_Intersects(AA.location, ST_GeomFromText('LINESTRING(-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0)',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'point test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_16_Point

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE ST_Intersects(AA.location, ST_GeomFromText('POINT(10.0 -10.0)',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'multiPoint test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_GEOMETRY
        def filter = CqlFilterExamples.EXAMPLE_16_MultiPoint

        when:
        String expected = "A.id IN (SELECT AA.id FROM building AA WHERE ST_Intersects(AA.location, ST_GeomFromText('MULTIPOINT(10.0 -10.0,10.0 10.0)',4326)))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'temporal with list test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_DATE
        def filter = CqlFilterExamples.EXAMPLE_26

        when:
        String expected = "(A.id IN (SELECT AA.id FROM building AA WHERE AA.updated > TIMESTAMP '2017-06-10T07:30:00Z') AND A.id IN (SELECT AA.id FROM building AA WHERE AA.updated < TIMESTAMP 'infinity'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'temporal with list and not test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_DATE
        def filter = Not.of(CqlFilterExamples.EXAMPLE_26)

        when:
        String expected = "(A.id IN (SELECT AA.id FROM building AA WHERE AA.updated <= TIMESTAMP '2017-06-10T07:30:00Z') OR A.id IN (SELECT AA.id FROM building AA WHERE AA.updated >= TIMESTAMP 'infinity'))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'function test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_DATE
        def filter = CqlFilterExamples.EXAMPLE_43

        when:
        String expected = "row_number BETWEEN 4 AND 8"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'function 2 test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_DATE
        def filter = CqlFilterExamples.EXAMPLE_31

        when:
        String expected = "year(TIMESTAMP '2012-06-05T00:00:00Z') = 2012"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'function 2 test with not'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_DATE
        def filter = Not.of(CqlFilterExamples.EXAMPLE_31)

        when:
        String expected = "year(TIMESTAMP '2012-06-05T00:00:00Z') <> 2012"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected

    }

    def 'nested not test'() {

        given:
        def instanceContainer = QuerySchemaFixtures.SIMPLE_DATE
        def filter = CqlFilterExamples.EXAMPLE_NOT

        when:
        String expected = "(A.id IN (SELECT AA.id FROM building AA WHERE AA.test = 1) OR (A.id IN (SELECT AA.id FROM building AA WHERE AA.test1 <> 1) AND A.id IN (SELECT AA.id FROM building AA WHERE AA.test2 = 'foo') AND A.id IN (SELECT AA.id FROM building AA WHERE AA.test3 <= 'bar')) OR (A.id IN (SELECT AA.id FROM building AA WHERE AA.test1 <> 1) OR A.id IN (SELECT AA.id FROM building AA WHERE AA.test2 = 'foo') OR A.id IN (SELECT AA.id FROM building AA WHERE AA.test3 <= 'bar')) OR A.id IN (SELECT AA.id FROM building AA WHERE AA.test <> 1))"

        String actual = filterEncoder.encode(filter, instanceContainer)

        then:

        actual == expected
    }

}
