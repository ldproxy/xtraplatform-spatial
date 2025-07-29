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

    ToSimpleFeatures toSf = new ToSimpleFeatures()

    def 'POINT XY'() {

        given:
        Geometry<?> geometry = Point.of(Position.ofXY(10.81, 10.37))

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
        Geometry<?> geometry = LineString.of(PositionList.of(Axes.XY, new double[]{10.0,10.0,20.0,20.0,30.0,40.0}));

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
        Geometry<?> geometry = MultiPoint.of(List.of(Point.of(Position.ofXY(10,10)),Point.of(Position.ofXY(20,20))))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'POLYGON XY'() {
        given:
        Geometry<?> geometry = Polygon.of(List.of(PositionList.of(Axes.XY,new double[]{10.0,10.0,20.0,20.0,30.0,40.0,10.0,10.0})))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'MULTILINESTRING XY'() {
        given:
        Geometry<?> geometry = MultiLineString.of(List.of(
                LineString.of(PositionList.of(Axes.XY, new double[]{10.0,10.0,20.0,20.0})),
                LineString.of(PositionList.of(Axes.XY, new double[]{30.0,40.0,50.0,60.0}))
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
        Geometry<?> geometry = GeometryCollection.of(List.of(Point.of(Position.ofXY(10,10)),LineString.of(PositionList.of(Axes.XY, new double[]{20.0,20.0,30.0,30.0}))))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry.toString() == geometry.toString()
    }

    def 'Nested GEOMETRYCOLLECTION with Point and LineString / MultiPoint'() {
        given:
        Geometry<?> multiPoint = MultiPoint.of(List.of(Point.of(Position.ofXY(10,10)),Point.of(Position.ofXY(20,20))))
        Geometry<?> geometryCollection = GeometryCollection.of(List.of(Point.of(Position.ofXY(10,10)),LineString.of(PositionList.of(Axes.XY, new double[]{20.0,20.0,30.0,30.0}))))
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
        sfGeometry == LineString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 2.0, 0.0}))
    }

    def 'COMPOUNDCURVE XY'() {
        given:
        Geometry<?> geometry = CompoundCurve.of(List.of(
                LineString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0})),
                LineString.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 0.0}))
        ))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry == LineString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 2.0, 0.0}))
    }

    def 'CURVEPOLYGON XY'() {
        given:
        Geometry<?> geometry = CurvePolygon.of(List.of(
                CircularString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 0.0, 2.0, -1.0, -1.0, 0.0, 0.0}))
        ))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry == Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 0.0, 2.0, -1.0, -1.0, 0.0, 0.0})))
    }

    def 'MULTICURVE XY'() {
        given:
        Geometry<?> geometry = MultiCurve.of(List.of(
                LineString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0})),
                CircularString.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 0.0, 3.0, 1.0}))
        ))

        when:
        Geometry<?> sfGeometry =geometry.accept(toSf)

        then:
        sfGeometry == MultiLineString.of(List.of(
                LineString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0})),
                LineString.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 0.0, 3.0, 1.0}))
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
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 2.0, 3, 2, 2.0, 1.0, 1.0, 1.0})))
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
