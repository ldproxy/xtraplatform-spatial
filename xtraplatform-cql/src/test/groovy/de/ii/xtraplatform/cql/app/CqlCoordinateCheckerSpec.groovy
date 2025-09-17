/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app


import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry
import de.ii.xtraplatform.blobs.domain.ResourceStore
import de.ii.xtraplatform.cql.domain.*
import de.ii.xtraplatform.crs.domain.CrsInfo
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory
import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.crs.infra.CrsTransformerFactoryProj
import de.ii.xtraplatform.geometries.domain.Axes
import de.ii.xtraplatform.geometries.domain.MultiLineString
import de.ii.xtraplatform.geometries.domain.LineString
import de.ii.xtraplatform.geometries.domain.MultiPolygon
import de.ii.xtraplatform.geometries.domain.Polygon
import de.ii.xtraplatform.geometries.domain.Point
import de.ii.xtraplatform.geometries.domain.MultiPoint
import de.ii.xtraplatform.geometries.domain.PositionList
import de.ii.xtraplatform.proj.domain.ProjLoaderImpl
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Path
import java.util.concurrent.CompletableFuture

class CqlCoordinateCheckerSpec extends Specification {

    @Shared
    Cql cql

    @Shared
    ResourceStore resourceStore

    @Shared
    VolatileRegistry volatileRegistry

    @Shared
    CrsTransformerFactoryProj transformerFactory

    @Shared
    CqlCoordinateChecker visitor1

    @Shared
    CqlCoordinateChecker visitor2

    @Shared
    CqlCoordinateChecker visitor3


    def setupSpec() {
        cql = new CqlImpl()
        resourceStore = Stub()
        volatileRegistry = Stub()
        volatileRegistry.onAvailable(*_) >> CompletableFuture.completedFuture(null)
        transformerFactory = new CrsTransformerFactoryProj(new ProjLoaderImpl(Path.of(System.getProperty("java.io.tmpdir"), "proj", "data")), resourceStore, volatileRegistry)
        transformerFactory.onStart(false).toCompletableFuture().join()
        visitor1 = new CqlCoordinateChecker((CrsTransformerFactory) transformerFactory, (CrsInfo) transformerFactory, OgcCrs.CRS84, EpsgCrs.of(5555))
        visitor2 = new CqlCoordinateChecker((CrsTransformerFactory) transformerFactory, (CrsInfo) transformerFactory, OgcCrs.CRS84, OgcCrs.CRS84)
        visitor3 = new CqlCoordinateChecker((CrsTransformerFactory) transformerFactory, (CrsInfo) transformerFactory, EpsgCrs.of(25830), EpsgCrs.of(5555))
    }


    def 'Valid CRS84 bbox'() {
        given:
        //

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Bbox.of(6,48,12, 52,OgcCrs.CRS84))).accept(visitor1)

        then:
        noExceptionThrown()

        and:

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Bbox.of(6,48,12, 52,OgcCrs.CRS84))).accept(visitor2)

        then:
        noExceptionThrown()
    }

    def "Native not present"() {
        when:
        CrsTransformerFactoryProj transformerFactoryOptionalEmpty = new CrsTransformerFactoryProj(new ProjLoaderImpl(Path.of(System.getProperty("java.io.tmpdir"), "proj", "data")), resourceStore, volatileRegistry)
        CqlCoordinateChecker visitor1OptionalEmpty = new CqlCoordinateChecker((CrsTransformerFactory) transformerFactoryOptionalEmpty, (CrsInfo) transformerFactoryOptionalEmpty, OgcCrs.CRS84, null)
        CqlCoordinateChecker visitor2OptionalEmpty = new CqlCoordinateChecker((CrsTransformerFactory) transformerFactoryOptionalEmpty, (CrsInfo) transformerFactoryOptionalEmpty, OgcCrs.CRS84, null)
        then:
        visitor1OptionalEmpty.crsTransformerFilterToNative.isEmpty()
        visitor2OptionalEmpty.crsTransformerFilterToNative.isEmpty()
    }

    def 'Test crsTransformerFilterToCrs84 is present'() {

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Bbox.of(6,52,12, 53, OgcCrs.CRS84))).accept(visitor3)

        then:
        thrown IllegalArgumentException

    }

    def 'min > max'() {
        given:
        //

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Bbox.of(6,53,12, 52,OgcCrs.CRS84))).accept(visitor1)

        then:
        thrown IllegalArgumentException

        and:

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Bbox.of(6,53,12, 52,OgcCrs.CRS84))).accept(visitor2)

        then:

        thrown IllegalArgumentException
    }

    def 'lat > 90'() {
        given:
        //

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Bbox.of(6,10,12, 100,OgcCrs.CRS84))).accept(visitor1)

        then:
        thrown IllegalArgumentException

        and:

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Bbox.of(6,10,12, 100,OgcCrs.CRS84))).accept(visitor2)

        then:
        thrown IllegalArgumentException
    }

    def 'crossing antimeridian'() {
        given:
        //

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Bbox.of(160,-30,-160, 10,OgcCrs.CRS84))).accept(visitor1)

        then:
        thrown IllegalArgumentException

        and:

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Bbox.of(160,-30,-160, 10,OgcCrs.CRS84))).accept(visitor2)

        then:
        noExceptionThrown()
    }

    def 'Basic test'() {
        given:
        String cqlText = "S_INTERSECTS(\"bbox\",POLYGON((6.841676292954137 51.530481422116154,6.910590039820299 51.48142884149749,6.920492602491171 51.47642346300475,6.92048790793338 51.48838138815214,6.920711061968184 51.49719648216943,6.9155177499026191 51.49619902382169,6.915465875139691 51.51941060843487,6.909367346948651 51.52980833786589,6.906686188338441 51.536126767037025,6.841676292954137 51.530481422116154)))"

        when:
        def test = cql.read(cqlText, Cql.Format.TEXT).accept(visitor1)

        then:
        noExceptionThrown()
    }

    def "Visit with Point test" (){
        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Point.of(5.00, 42.27))).accept(visitor1)

        then:
        thrown IllegalArgumentException

        and:

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(Point.of(6.00, 47.27))).accept(visitor1)

        then:
        noExceptionThrown()
    }


    def "Visit with LineString test" () {

        when:

        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(LineString.of(new double[]{(double)6.00, (double)47.27, (double)7.00, (double)48.27}))).accept(visitor1)

        then:
        noExceptionThrown()

        and:

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(spatialLiteral)).accept(visitor1)

        then:
        thrown ex

        where:

        xCoordinate     |    yCoordinate     |      spatialLiteral                                                              |       ex
        (double) 8.00   |    (double) 5      |     LineString.of(new double[]{xCoordinate, yCoordinate, xCoordinate, yCoordinate}) |       IllegalArgumentException
        (double) 0.50   |    (double) 1000   |     LineString.of(new double[]{xCoordinate, yCoordinate, xCoordinate, yCoordinate}) |       NullPointerException

    }

    def "Test visit MultiPoint"(){
        when:

        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(MultiPoint.of(List.of(Point.of((double)12.00, (double)55.09), Point.of((double)11.00, (double)54.09))))).accept(visitor1)

        then:

        noExceptionThrown()

        and:

        when:

        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(spatialLiteral)).accept(visitor1)

        then:

        thrown ex

        where:

        xCoordinate     |    yCoordinate     |      spatialLiteral                                                        |       ex
        (double) 11.00  |    (double) 56.00  |     MultiPoint.of(List.of(Point.of(xCoordinate, yCoordinate))) |       IllegalArgumentException
        (double) 0.50   |    (double) 1000   |     MultiPoint.of(List.of(Point.of(xCoordinate, yCoordinate))) |       NullPointerException

    }

    def "Test visit MultiLineString"(){

        when:

        LineString lineString1 =  LineString.of(new double[]{(double)6.00, (double)47.27, (double)11.00, (double)50.27})
        LineString lineString2 =  LineString.of(new double[]{(double)6.00, (double)47.27, (double)11.00, (double)50.27})

        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(MultiLineString.of(List.of(lineString1, lineString2)))).accept(visitor1)

        then:
        noExceptionThrown()

        and:

        when:
        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(spatialLiteral)).accept(visitor1)

        then:
        thrown ex

        where:

        xCoordinate1    |    yCoordinate1   |    xCoordinate2   |    yCoordinate2        |      spatialLiteral                                                                                                                                                |       ex
        (double) 8.00   |    (double) 5     |    (double) 4.40  |    (double) 51.00      |     MultiLineString.of(List.of(LineString.of(xCoordinate1, yCoordinate1, xCoordinate2, yCoordinate2))) |       IllegalArgumentException
        (double) 0.50   |    (double) 1000  |    (double) 0.03  |    (double) 1          |     MultiLineString.of(List.of(LineString.of(xCoordinate1, yCoordinate1, xCoordinate2, yCoordinate2))) |       NullPointerException

    }

    def "Test MultiPolygon visit"(){

        when:

        Polygon polygon1 =  Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{(double)6.00, (double)47.27, (double)11.00, (double)50.27, (double)11.00, (double)50.27, (double)6.00, (double)47.27})))

        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(MultiPolygon.of(List.of(polygon1)))).accept(visitor1)

        then:
        noExceptionThrown()

        and:

        when:
        Polygon polygon2 =  Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{xCoordinate1, yCoordinate1, xCoordinate2, yCoordinate2, xCoordinate2, yCoordinate2, xCoordinate1, yCoordinate1})))
        Polygon polygon3 =  Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{xCoordinate3, yCoordinate3, xCoordinate4, yCoordinate4, xCoordinate4, yCoordinate4, xCoordinate3, yCoordinate3})))

        SIntersects.of(Property.of("bbox"), SpatialLiteral.of(MultiPolygon.of(List.of(polygon2, polygon3)))).accept(visitor1)

        then:
        thrown ex

        where:

        xCoordinate1    |    yCoordinate1   |    xCoordinate2   |    yCoordinate2      |  xCoordinate3    |    yCoordinate3   |    xCoordinate4   |    yCoordinate4    |     ex
        (double) 8.00   |    (double) 5     |    (double) 4.40  |    (double) 51.00    |  (double) 8.00   |    (double) 5     |    (double) 4.40  |    (double) 51.00  |     IllegalArgumentException
        (double) 0.50   |    (double) 1000  |    (double) 0.03  |    (double) 1        |  (double) 0.00  |    (double) 100000  |    (double) 0.03  |    (double) 1      |    NullPointerException

    }

    def 'Coordinates > Maximums crs'(){
        given:

        List<Optional<Double>> doubles1 = new ArrayList<>()
        doubles1.add(Optional.of((double)1.00))
        doubles1.add(Optional.of((double)2.00))

        List<Optional<Double>> doubles2 = new ArrayList<>()
        doubles2.add(Optional.of((double)3.00))
        doubles2.add(Optional.of((double)4.00))



        def transformerFactory3 = Mock(CrsTransformerFactoryProj)
        transformerFactory3.getAxisMinimums(_) >> doubles1
        transformerFactory3.getAxisMaximums(_) >> doubles2

        CqlCoordinateChecker visitor4 = new CqlCoordinateChecker((CrsTransformerFactory) transformerFactory3, (CrsInfo) transformerFactory3, OgcCrs.CRS84, EpsgCrs.of(5555))

        when:

        double xmin = 6.00
        double ymin = 6.00
        double xmax = 12.00
        double ymax = 12.00

        SpatialLiteral.of(Bbox.of(xmin, ymin, xmax, ymax, OgcCrs.CRS84)).accept(visitor4)



        then:

        thrown IllegalArgumentException

    }

    def 'Coordinate < Minimums crs'(){
        given:

        List<Optional<Double>> doubles1 = new ArrayList<>()
        doubles1.add(Optional.of((double)1.00))
        doubles1.add(Optional.of((double)2.00))

        List<Optional<Double>> doubles2 = new ArrayList<>()
        doubles2.add(Optional.of((double)3.00))
        doubles2.add(Optional.of((double)4.00))



        def transformerFactory4 = Mock(CrsTransformerFactoryProj)
        transformerFactory4.getAxisMinimums(_) >> doubles1
        transformerFactory4.getAxisMaximums(_) >> doubles2

        CqlCoordinateChecker visitor4 = new CqlCoordinateChecker((CrsTransformerFactory) transformerFactory4, (CrsInfo) transformerFactory4, OgcCrs.CRS84, EpsgCrs.of(5555))

        when:

        double xmin = 0.50
        double ymin = 0.50
        double xmax = 2.00
        double ymax = 2.00

        SpatialLiteral.of(Bbox.of(xmin, ymin, xmax, ymax, OgcCrs.CRS84)).accept(visitor4)



        then:

        thrown IllegalArgumentException

    }

}

