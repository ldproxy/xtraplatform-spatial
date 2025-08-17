/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain


import de.ii.xtraplatform.geometries.domain.transform.ToSimpleFeatures
import spock.lang.Specification

class ToSimpleFeaturesSpec extends Specification {

    ToSimpleFeatures toSf = new ToSimpleFeatures(0.1)

    def 'POINT XY'() {

        given:
        Geometry<?> geometry = Point.of(10.81, 10.37)

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'POINT XYZ in CRS84h'() {
        given:
        Geometry<?> geometry = Point.of(Position.ofXYZ(10.81, 10.37, 5.00))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'POINT XYM'() {
        given:
        Geometry<?> geometry = Point.of(Position.ofXYM(10.81, 10.37, 5.00))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'POINT XYZM'() {
        given:
        Geometry<?> geometry = Point.of(Position.ofXYZM(10.81, 10.37, 5.00, 7.50))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'POINT XY EMPTY'() {
        given:
        Geometry<?> geometry = Point.empty(Axes.XY);

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'POINT XYZ EMPTY'() {
        given:
        Geometry<?> geometry = Point.empty(Axes.XYZ);

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'LINESTRING XY'() {
        given:
        Geometry<?> geometry = LineString.of(new double[]{10.0,10.0,20.0,20.0,30.0,40.0});

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'LINESTRING XYZ'() {
        given:
        Geometry<?> geometry = LineString.of(PositionList.of(Axes.XYZ, new double[]{10.0,10.0,1.0,20.0,20.0,2.0,30.0,40.0,3.0}));

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'MULTIPOINT XY'() {
        given:
        Geometry<?> geometry = MultiPoint.of(List.of(Point.of(10,10),Point.of(20,20)))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'POLYGON XY'() {
        given:
        Geometry<?> geometry = Polygon.of(new double[]{10.0,10.0,20.0,20.0,30.0,40.0,10.0,10.0})

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'MULTILINESTRING XY'() {
        given:
        Geometry<?> geometry = MultiLineString.of(List.of(
                LineString.of(new double[]{10.0,10.0,20.0,20.0}),
                LineString.of(new double[]{30.0,40.0,50.0,60.0})
        ))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'MULTIPOLYGON XY'() {
        given:
        Geometry<?> geometry = MultiPolygon.of(List.of(Polygon.of(List.of(PositionList.of(Axes.XY,new double[]{10.0,10.0,20.0,20.0,30.0,40.0,10.0,10.0}))), Polygon.of(List.of(PositionList.of(Axes.XY,new double[]{50.0,50.0,60.0,60.0,70.0,80.0,50.0,50.0})))))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'GEOMETRYCOLLECTION with Point and LineString'() {
        given:
        Geometry<?> geometry = GeometryCollection.of(List.of(Point.of(10,10),LineString.of(new double[]{20.0,20.0,30.0,30.0})))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'Nested GEOMETRYCOLLECTION with Point and LineString / MultiPoint'() {
        given:
        Geometry<?> multiPoint = MultiPoint.of(List.of(Point.of(10,10),Point.of(20,20)))
        Geometry<?> geometryCollection = GeometryCollection.of(List.of(Point.of(10,10),LineString.of(new double[]{20.0,20.0,30.0,30.0})))
        Geometry<?> geometry = GeometryCollection.of(List.of(geometryCollection, multiPoint))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'POLYHEDRALSURFACE XY'() {
        given:
        Geometry geometry = PolyhedralSurface.of(List.of(
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0}))),
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0})))
        ))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry == MultiPolygon.of(List.of(
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0}))),
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0})))
        ))

    }

    def 'CIRCULARSTRING XY'() {
        given:
        Geometry<?> geometry = CircularString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 2.0, 0.0}))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry == LineString.of(new double[]{0.0, 0.0, 0.09903113209758097, 0.43388373911755823, 0.3765101981412665, 0.7818314824680299, 0.7774790660436857, 0.9749279121818236, 1.2225209339563143, 0.9749279121818236, 1.6234898018587336, 0.7818314824680298, 1.900968867902419, 0.4338837391175581, 2.0, 0.0})
    }

    def 'COMPOUNDCURVE XY'() {
        given:
        Geometry<?> geometry = CompoundCurve.of(List.of(
                LineString.of(new double[]{0.0, 0.0, 1.0, 1.0}),
                LineString.of(new double[]{1.0, 1.0, 2.0, 0.0})
        ))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry == LineString.of(new double[]{0.0, 0.0, 1.0, 1.0, 2.0, 0.0})
    }

    def 'CURVEPOLYGON XY'() {
        given:
        Geometry<?> geometry = CurvePolygon.of(List.of(
                CircularString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 0.0, 2.0, -1.0, -1.0, 0.0, 0.0}))
        ))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry == Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 0.4338837391175582, 0.09903113209758085, 0.7818314824680298, 0.3765101981412665, 0.9749279121818236, 0.7774790660436857, 0.9749279121818236, 1.2225209339563143, 0.7818314824680298, 1.6234898018587334, 0.4338837391175582, 1.900968867902419, 0.0, 2.0, -0.47566316984739254, 2.6359698127533715, -1.143610167153111, 3.065574122174384, -1.919584413816641, 3.2346215190718066, -2.7057024924684696, 3.1217879234560155, -3.4028014162887956, 2.741306459661294, -3.922947307057388, 2.1411720528827987, -4.200527604127995, 1.3970872240084133, -4.200527604127995, 0.6029127759915882, -3.9229473070573886, -0.1411720528827971, -3.402801416288796, -0.7413064596612935, -2.705702492468472, -1.1217879234560146, -1.9195844138166434, -1.2346215190718066, -1.1436101671531114, -1.0655741221743842, -0.4756631698473943, -0.6359698127533733, 0.0, 0.0})))
    }

    def 'MULTICURVE XY'() {
        given:
        Geometry<?> geometry = MultiCurve.of(List.of(
                LineString.of(new double[]{0.0, 0.0, 1.0, 1.0}),
                CircularString.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 0.0, 3.0, 1.0}))
        ))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry == MultiLineString.of(List.of(
                LineString.of(new double[]{0.0, 0.0, 1.0, 1.0}),
                LineString.of(new double[]{1.0, 1.0, 1.099031132097581, 0.5661162608824419, 1.3765101981412662, 0.2181685175319703, 1.7774790660436854, 0.02507208781817649, 2.2225209339563143, 0.02507208781817627, 2.6234898018587334, 0.21816851753197009, 2.900968867902419, 0.5661162608824417, 3.0, 1.0})
        ))
    }

    def 'MULTISURFACE XY'() {
        given:
        Geometry<?> geometry = MultiSurface.of(List.of(
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0}))),
                CurvePolygon.of(List.of(CircularString.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 2.0, 3, 2, 2.0, 1.0, 1.0, 1.0}))))
        ))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry == MultiPolygon.of(List.of(
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0}))),
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 1.4509618943233418, 1.6830127018922194, 2.183012701892219, 2.049038105676658, 3.0, 2.0, 2.549038105676658, 1.3169872981077806, 1.8169872981077806, 0.9509618943233418, 1.0, 1.0})))
        ))
    }

    def 'POLYHEDRALSURFACE XYZ Solid'() {
        given:
        Geometry geometry = PolyhedralSurface.of(List.of(
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280414.631, 5660090.756, 40.255, 280414.631, 5660088.454, 40.255, 280414.631, 5660088.454, 32.967, 280414.631, 5660090.756, 32.967, 280414.631, 5660090.756, 40.255}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280414.631, 5660088.454, 40.255, 280405.623, 5660088.454, 33.256, 280405.623, 5660088.454, 32.967, 280414.631, 5660088.454, 32.967, 280414.631, 5660088.454, 40.255}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280405.623, 5660088.454, 33.256, 280405.623, 5660090.756, 33.256, 280405.623, 5660090.756, 32.967, 280405.623, 5660088.454, 32.967, 280405.623, 5660088.454, 33.256}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280405.623, 5660090.756, 33.256, 280414.631, 5660090.756, 40.255, 280414.631, 5660090.756, 32.967, 280405.623, 5660090.756, 32.967, 280405.623, 5660090.756, 33.256}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280405.623, 5660088.454, 33.256, 280414.631, 5660088.454, 40.255, 280411.722, 5660088.454, 41.63, 280405.623, 5660088.454, 33.256}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280414.631, 5660090.756, 40.255, 280405.623, 5660090.756, 33.256, 280411.722, 5660090.756, 41.63, 280414.631, 5660090.756, 40.255}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280414.631, 5660088.454, 40.255, 280414.631, 5660090.756, 40.255, 280411.722, 5660090.756, 41.63, 280411.722, 5660088.454, 41.63, 280414.631, 5660088.454, 40.255}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280405.623, 5660090.756, 33.256, 280405.623, 5660088.454, 33.256, 280411.722, 5660088.454, 41.63, 280411.722, 5660090.756, 41.63, 280405.623, 5660090.756, 33.256}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280414.631, 5660090.756, 32.967, 280414.631, 5660088.454, 32.967, 280405.623, 5660088.454, 32.967, 280405.623, 5660090.756, 32.967, 280414.631, 5660090.756, 32.967})))
        ), true)

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry == MultiPolygon.of(List.of(
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280414.631, 5660090.756, 40.255, 280414.631, 5660088.454, 40.255, 280414.631, 5660088.454, 32.967, 280414.631, 5660090.756, 32.967, 280414.631, 5660090.756, 40.255}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280414.631, 5660088.454, 40.255, 280405.623, 5660088.454, 33.256, 280405.623, 5660088.454, 32.967, 280414.631, 5660088.454, 32.967, 280414.631, 5660088.454, 40.255}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280405.623, 5660088.454, 33.256, 280405.623, 5660090.756, 33.256, 280405.623, 5660090.756, 32.967, 280405.623, 5660088.454, 32.967, 280405.623, 5660088.454, 33.256}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280405.623, 5660090.756, 33.256, 280414.631, 5660090.756, 40.255, 280414.631, 5660090.756, 32.967, 280405.623, 5660090.756, 32.967, 280405.623, 5660090.756, 33.256}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280405.623, 5660088.454, 33.256, 280414.631, 5660088.454, 40.255, 280411.722, 5660088.454, 41.63, 280405.623, 5660088.454, 33.256}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280414.631, 5660090.756, 40.255, 280405.623, 5660090.756, 33.256, 280411.722, 5660090.756, 41.63, 280414.631, 5660090.756, 40.255}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280414.631, 5660088.454, 40.255, 280414.631, 5660090.756, 40.255, 280411.722, 5660090.756, 41.63, 280411.722, 5660088.454, 41.63, 280414.631, 5660088.454, 40.255}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280405.623, 5660090.756, 33.256, 280405.623, 5660088.454, 33.256, 280411.722, 5660088.454, 41.63, 280411.722, 5660090.756, 41.63, 280405.623, 5660090.756, 33.256}))),
                Polygon.of(List.of(PositionList.of(Axes.XYZ, new double[]{280414.631, 5660090.756, 32.967, 280414.631, 5660088.454, 32.967, 280405.623, 5660088.454, 32.967, 280405.623, 5660090.756, 32.967, 280414.631, 5660090.756, 32.967})))
        ))
    }
}
