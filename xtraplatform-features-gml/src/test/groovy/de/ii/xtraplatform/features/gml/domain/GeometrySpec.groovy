/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain

import de.ii.xtraplatform.geometries.domain.Axes
import de.ii.xtraplatform.geometries.domain.CircularString
import de.ii.xtraplatform.geometries.domain.CompoundCurve
import de.ii.xtraplatform.geometries.domain.CurvePolygon
import de.ii.xtraplatform.geometries.domain.Geometry
import de.ii.xtraplatform.geometries.domain.GeometryCollection
import de.ii.xtraplatform.geometries.domain.LineString
import de.ii.xtraplatform.geometries.domain.MultiCurve
import de.ii.xtraplatform.geometries.domain.MultiLineString
import de.ii.xtraplatform.geometries.domain.MultiPoint
import de.ii.xtraplatform.geometries.domain.MultiPolygon
import de.ii.xtraplatform.geometries.domain.MultiSurface
import de.ii.xtraplatform.geometries.domain.Point
import de.ii.xtraplatform.geometries.domain.Polygon
import de.ii.xtraplatform.geometries.domain.PolyhedralSurface
import de.ii.xtraplatform.geometries.domain.Position
import de.ii.xtraplatform.geometries.domain.PositionList
import spock.lang.Specification

class GeometrySpec extends Specification {

    StringBuilder sb = new StringBuilder()
    GeometryEncoderGml gmlEncoderWith = new GeometryEncoderGml(sb, Set.of(GeometryEncoderGml.Options.WITH_GML_ID, GeometryEncoderGml.Options.WITH_SRS_NAME), Optional.of("gml"), Optional.of("g_"), List.of(1,1))
    GeometryEncoderGml gmlEncoderWithout = new GeometryEncoderGml(sb, Set.of(), Optional.of("gml"), Optional.empty(), List.of())

    def 'POINT XY'() {

        given:
        Geometry<?> geometry = Point.of(Position.ofXY(10.81, 10.37))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut1 = sb.toString()
        sb.setLength(0)
        geometry.accept(gmlEncoderWith)
        String gmlOut2 = sb.toString()

        then:
        gmlOut1 == "<gml:Point><gml:pos>10.81 10.37</gml:pos></gml:Point>"
        gmlOut2 == "<gml:Point gml:id=\"g_0\" srsName=\"http://www.opengis.net/def/crs/OGC/1.3/CRS84\"><gml:pos>10.8 10.4</gml:pos></gml:Point>"
    }

    def 'POINT XYZ in CRS84h'() {
        given:
        Geometry<?> geometry = Point.of(Position.ofXYZ(10.81, 10.37, 5.00))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:Point><gml:pos>10.81 10.37 5.0</gml:pos></gml:Point>"
    }

    def 'POINT XYM'() {
        given:
        Geometry<?> geometry = Point.of(Position.ofXYM(10.81, 10.37, 5.00))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:Point><gml:pos>10.81 10.37</gml:pos></gml:Point>"
    }

    def 'POINT XYZM'() {
        given:
        Geometry<?> geometry = Point.of(Position.ofXYZM(10.81, 10.37, 5.00, 7.50))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:Point><gml:pos>10.81 10.37 5.0</gml:pos></gml:Point>"
    }

    def 'POINT XY EMPTY'() {
        given:
        Geometry<?> geometry = Point.empty(Axes.XY);

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        // TODO what should be serialized for empty points?
        gmlOut == "<gml:Point><gml:pos>NaN NaN</gml:pos></gml:Point>"
    }

    def 'POINT XYZ EMPTY'() {
        given:
        Geometry<?> geometry = Point.empty(Axes.XYZ);

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:Point><gml:pos>NaN NaN NaN</gml:pos></gml:Point>"
    }

    def 'LINESTRING XY'() {
        given:
        Geometry<?> geometry = LineString.of(PositionList.of(Axes.XY, new double[]{10.0,10.0,20.0,20.0,30.0,40.0}));

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:LineString><gml:posList>10.0 10.0 20.0 20.0 30.0 40.0</gml:posList></gml:LineString>"
    }

    def 'LINESTRING XYZ'() {
        given:
        Geometry<?> geometry = LineString.of(PositionList.of(Axes.XYZ, new double[]{10.0,10.0,1.0,20.0,20.0,2.0,30.0,40.0,3.0}));

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:LineString><gml:posList>10.0 10.0 1.0 20.0 20.0 2.0 30.0 40.0 3.0</gml:posList></gml:LineString>"
    }

    def 'MULTIPOINT XY'() {
        given:
        Geometry<?> geometry = MultiPoint.of(List.of(Point.of(Position.ofXY(10,10)),Point.of(Position.ofXY(20,20))))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:MultiPoint><gml:pointMember><gml:Point><gml:pos>10.0 10.0</gml:pos></gml:Point></gml:pointMember><gml:pointMember><gml:Point><gml:pos>20.0 20.0</gml:pos></gml:Point></gml:pointMember></gml:MultiPoint>"
    }

    def 'POLYGON XY'() {
        given:
        Geometry<?> geometry = Polygon.of(List.of(PositionList.of(Axes.XY,new double[]{10.0,10.0,20.0,20.0,30.0,40.0,10.0,10.0})))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>10.0 10.0 20.0 20.0 30.0 40.0 10.0 10.0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>"
    }

    def 'MULTILINESTRING XY'() {
        given:
        Geometry<?> geometry = MultiLineString.of(List.of(
                LineString.of(PositionList.of(Axes.XY, new double[]{10.0,10.0,20.0,20.0})),
                LineString.of(PositionList.of(Axes.XY, new double[]{30.0,40.0,50.0,60.0}))
        ))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:MultiCurve><gml:curveMember><gml:LineString><gml:posList>10.0 10.0 20.0 20.0</gml:posList></gml:LineString></gml:curveMember><gml:curveMember><gml:LineString><gml:posList>30.0 40.0 50.0 60.0</gml:posList></gml:LineString></gml:curveMember></gml:MultiCurve>"
    }

    def 'MULTIPOLYGON XY'() {
        given:
        Geometry<?> geometry = MultiPolygon.of(List.of(Polygon.of(List.of(PositionList.of(Axes.XY,new double[]{10.0,10.0,20.0,20.0,30.0,40.0,10.0,10.0}))), Polygon.of(List.of(PositionList.of(Axes.XY,new double[]{50.0,50.0,60.0,60.0,70.0,80.0,50.0,50.0})))))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:MultiSurface><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>10.0 10.0 20.0 20.0 30.0 40.0 10.0 10.0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>50.0 50.0 60.0 60.0 70.0 80.0 50.0 50.0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>"
    }

    def 'GEOMETRYCOLLECTION with Point and LineString'() {
        given:
        Geometry<?> geometry = GeometryCollection.of(List.of(Point.of(Position.ofXY(10,10)),LineString.of(PositionList.of(Axes.XY, new double[]{20.0,20.0,30.0,30.0}))))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:MultiGeometry><gml:geometryMember><gml:Point><gml:pos>10.0 10.0</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:LineString><gml:posList>20.0 20.0 30.0 30.0</gml:posList></gml:LineString></gml:geometryMember></gml:MultiGeometry>"
    }

    def 'Nested GEOMETRYCOLLECTION with Point and LineString / MultiPoint'() {
        given:
        Geometry<?> multiPoint = MultiPoint.of(List.of(Point.of(Position.ofXY(10,10)),Point.of(Position.ofXY(20,20))))
        Geometry<?> geometryCollection = GeometryCollection.of(List.of(Point.of(Position.ofXY(10,10)),LineString.of(PositionList.of(Axes.XY, new double[]{20.0,20.0,30.0,30.0}))))
        Geometry<?> geometry = GeometryCollection.of(List.of(geometryCollection, multiPoint))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:MultiGeometry><gml:geometryMember><gml:MultiGeometry><gml:geometryMember><gml:Point><gml:pos>10.0 10.0</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:LineString><gml:posList>20.0 20.0 30.0 30.0</gml:posList></gml:LineString></gml:geometryMember></gml:MultiGeometry></gml:geometryMember><gml:geometryMember><gml:MultiPoint><gml:pointMember><gml:Point><gml:pos>10.0 10.0</gml:pos></gml:Point></gml:pointMember><gml:pointMember><gml:Point><gml:pos>20.0 20.0</gml:pos></gml:Point></gml:pointMember></gml:MultiPoint></gml:geometryMember></gml:MultiGeometry>"
    }

    def 'POLYHEDRALSURFACE XY'() {
        given:
        Geometry geometry = PolyhedralSurface.of(List.of(
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0}))),
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0})))
        ))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:PolyhedralSurface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList>0.0 0.0 0.0 1.0 1.0 1.0 0.0 0.0</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList>0.0 0.0 1.0 1.0 1.0 0.0 0.0 0.0</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:PolyhedralSurface>"
    }

    def 'CIRCULARSTRING XY'() {
        given:
        Geometry<?> geometry = CircularString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 2.0, 0.0}))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:Curve><gml:segments><gml:Arc><gml:posList>0.0 0.0 1.0 1.0 2.0 0.0</gml:posList></gml:Arc></gml:segments></gml:Curve>"
    }

    def 'COMPOUNDCURVE XY'() {
        given:
        Geometry<?> geometry = CompoundCurve.of(List.of(
                LineString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0})),
                LineString.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 0.0}))
        ))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>0.0 0.0 1.0 1.0</gml:posList></gml:LineStringSegment><gml:LineStringSegment><gml:posList>1.0 1.0 2.0 0.0</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>"
    }

    def 'CURVEPOLYGON XY'() {
        given:
        Geometry<?> geometry = CurvePolygon.of(List.of(
                CircularString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 0.0, 2.0, -1.0, -1.0, 0.0, 0.0}))
        ))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:Polygon><gml:exterior><gml:Ring><gml:curveMember><gml:Curve><gml:segments><gml:ArcString><gml:posList>0.0 0.0 1.0 1.0 0.0 2.0 -1.0 -1.0 0.0 0.0</gml:posList></gml:ArcString></gml:segments></gml:Curve></gml:curveMember></gml:Ring></gml:exterior></gml:Polygon>"
    }

    def 'MULTICURVE XY'() {
        given:
        Geometry<?> geometry = MultiCurve.of(List.of(
                LineString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0})),
                CircularString.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 0.0, 3.0, 1.0}))
        ))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:MultiCurve><gml:curveMember><gml:LineString><gml:posList>0.0 0.0 1.0 1.0</gml:posList></gml:LineString></gml:curveMember><gml:curveMember><gml:Curve><gml:segments><gml:Arc><gml:posList>1.0 1.0 2.0 0.0 3.0 1.0</gml:posList></gml:Arc></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>"
    }

    def 'MULTISURFACE XY'() {
        given:
        Geometry<?> geometry = MultiSurface.of(List.of(
                Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0}))),
                CurvePolygon.of(List.of(CircularString.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 2.0, 3, 2, 2.0, 1.0, 1.0, 1.0}))))
        ))

        when:
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:MultiSurface><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0.0 0.0 0.0 1.0 1.0 1.0 0.0 0.0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:Ring><gml:curveMember><gml:Curve><gml:segments><gml:ArcString><gml:posList>1.0 1.0 2.0 2.0 3.0 2.0 2.0 1.0 1.0 1.0</gml:posList></gml:ArcString></gml:segments></gml:Curve></gml:curveMember></gml:Ring></gml:exterior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>"
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
        sb.setLength(0)
        geometry.accept(gmlEncoderWithout)
        String gmlOut = sb.toString()

        then:
        gmlOut == "<gml:Solid><gml:exterior><gml:Shell><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>280414.631 5660090.756 40.255 280414.631 5660088.454 40.255 280414.631 5660088.454 32.967 280414.631 5660090.756 32.967 280414.631 5660090.756 40.255</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>280414.631 5660088.454 40.255 280405.623 5660088.454 33.256 280405.623 5660088.454 32.967 280414.631 5660088.454 32.967 280414.631 5660088.454 40.255</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>280405.623 5660088.454 33.256 280405.623 5660090.756 33.256 280405.623 5660090.756 32.967 280405.623 5660088.454 32.967 280405.623 5660088.454 33.256</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>280405.623 5660090.756 33.256 280414.631 5660090.756 40.255 280414.631 5660090.756 32.967 280405.623 5660090.756 32.967 280405.623 5660090.756 33.256</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>280405.623 5660088.454 33.256 280414.631 5660088.454 40.255 280411.722 5660088.454 41.63 280405.623 5660088.454 33.256</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>280414.631 5660090.756 40.255 280405.623 5660090.756 33.256 280411.722 5660090.756 41.63 280414.631 5660090.756 40.255</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>280414.631 5660088.454 40.255 280414.631 5660090.756 40.255 280411.722 5660090.756 41.63 280411.722 5660088.454 41.63 280414.631 5660088.454 40.255</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>280405.623 5660090.756 33.256 280405.623 5660088.454 33.256 280411.722 5660088.454 41.63 280411.722 5660090.756 41.63 280405.623 5660090.756 33.256</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>280414.631 5660090.756 32.967 280414.631 5660088.454 32.967 280405.623 5660088.454 32.967 280405.623 5660090.756 32.967 280414.631 5660090.756 32.967</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember></gml:Shell></gml:exterior></gml:Solid>"
    }
}
