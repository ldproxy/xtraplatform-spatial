/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain

import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryDecoderWkb
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryDecoderWkt
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryEncoderWkb
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryEncoderWkt
import spock.lang.Specification

class GeometryWktWkbSpec extends Specification {

    def 'POINT XY'() {

        given:

        String wkt = "POINT(10.81 10.37)"

        when:

        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        String wktOut1 = new GeometryEncoderWkt(List.of(2,2)).encode(geometry)
        String wktOut2 = new GeometryEncoderWkt(List.of(1,1)).encode(geometry)
        String wktOut3 = new GeometryEncoderWkt(List.of(3,3)).encode(geometry)

        then:
        geometry instanceof Point
        ((Point) geometry).getAxes() == Axes.XY
        ((Point) geometry).getType() == GeometryType.POINT
        ((Point) geometry).getValue() == Position.ofXY(10.81, 10.37)
        wktOut1 == wkt
        wktOut2 == "POINT(10.8 10.4)"
        wktOut3 == "POINT(10.810 10.370)"
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'POINT XYZ in CRS84h'() {
        given:
        String wkt = "POINT Z(10.81 10.37 5.00)"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt, Optional.of(OgcCrs.CRS84h))
        String wktOut1 = new GeometryEncoderWkt(List.of(2,2,2)).encode(geometry)
        String wktOut2 = new GeometryEncoderWkt(List.of(1,1,1)).encode(geometry)
        String wktOut3 = new GeometryEncoderWkt(List.of(3,0,3)).encode(geometry)
        then:
        geometry instanceof Point
        geometry.getType() == GeometryType.POINT
        geometry.getValue() == Position.ofXYZ(10.81, 10.37, 5.0)
        geometry.getAxes() == Axes.XYZ
        geometry.getCrs().get() == OgcCrs.CRS84h
        wktOut1 == wkt
        wktOut2 == "POINT Z(10.8 10.4 5.0)"
        wktOut3 == "POINT Z(10.810 10.37 5.000)"
        new GeometryEncoderWkt(List.of(2,2,2)).encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'POINT XYM'() {
        given:
        String wkt = "POINT M(10.81 10.37 7.5)"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        String wktOut1 = new GeometryEncoderWkt(List.of(2,2,1)).encode(geometry)
        String wktOut2 = new GeometryEncoderWkt(List.of(1,1,1)).encode(geometry)
        String wktOut3 = new GeometryEncoderWkt(List.of(3,0,3)).encode(geometry)
        then:
        geometry instanceof Point
        geometry.getType() == GeometryType.POINT
        geometry.getValue() == Position.ofXYM(10.81, 10.37, 7.5)
        geometry.getAxes() == Axes.XYM
        wktOut1 == wkt
        wktOut2 == "POINT M(10.8 10.4 7.5)"
        wktOut3 == "POINT M(10.810 10.37 7.500)"
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'POINT XYZM'() {
        given:
        String wkt = "POINT ZM(10.81 10.37 5.0 7.5)"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        String wktOut1 = new GeometryEncoderWkt(List.of(2,2,1,1)).encode(geometry)
        String wktOut2 = new GeometryEncoderWkt(List.of(1,1,1,1)).encode(geometry)
        String wktOut3 = new GeometryEncoderWkt(List.of(3,0,3,3)).encode(geometry)

        then:
        geometry instanceof Point
        geometry.getType() == GeometryType.POINT
        geometry.getValue() == Position.ofXYZM(10.81, 10.37, 5.0, 7.5)
        geometry.getAxes() == Axes.XYZM
        wktOut1 == wkt
        wktOut2 == "POINT ZM(10.8 10.4 5.0 7.5)"
        wktOut3 == "POINT ZM(10.810 10.37 5.000 7.500)"
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'POINT XYZ no space'() {
        given:
        String wkt = "POINTZ(10.81 10.33 5.0)"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry instanceof Point
        geometry.getType() == GeometryType.POINT
        geometry.getValue() == Position.ofXYZ(10.81, 10.33, 5.0)
        geometry.getAxes() == Axes.XYZ
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == "POINT Z(10.81 10.33 5.0)"
    }

    def 'POINT XYM no space'() {
        given:
        String wkt = "POINTM(10.81 10.33 7.5)"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry instanceof Point
        geometry.getType() == GeometryType.POINT
        geometry.getValue() == Position.ofXYM(10.81, 10.33, 7.5)
        geometry.getAxes() == Axes.XYM
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == "POINT M(10.81 10.33 7.5)"
    }

    def 'POINT XYZM no space'() {
        given:
        String wkt = "POINTZM(10.81 10.33 5.0 7.5)"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry instanceof Point
        geometry.getType() == GeometryType.POINT
        geometry.getValue() == Position.ofXYZM(10.81, 10.33, 5.0, 7.5)
        geometry.getAxes() == Axes.XYZM
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == "POINT ZM(10.81 10.33 5.0 7.5)"
    }

    def 'POINT XY EMPTY'() {

        given:

        String wkt = "POINT EMPTY"

        when:

        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        String wktOut1 = new GeometryEncoderWkt().encode(geometry)

        then:
        geometry instanceof Point
        ((Point) geometry).getAxes() == Axes.XY
        ((Point) geometry).getType() == GeometryType.POINT
        geometry.isEmpty()
        wktOut1 == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'POINT XYZ EMPTY'() {
        given:
        String wkt = "POINT Z EMPTY"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        String wktOut1 = new GeometryEncoderWkt().encode(geometry)
        then:
        geometry instanceof Point
        geometry.getType() == GeometryType.POINT
        geometry.getAxes() == Axes.XYZ
        geometry.isEmpty()
        wktOut1 == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'POINT XYM EMPTY'() {
        given:
        String wkt = "POINT M EMPTY"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        String wktOut1 = new GeometryEncoderWkt().encode(geometry)
        then:
        geometry instanceof Point
        geometry.getType() == GeometryType.POINT
        geometry.getAxes() == Axes.XYM
        geometry.isEmpty()
        wktOut1 == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'POINT XYZM EMPTY'() {
        given:
        String wkt = "POINT ZM EMPTY"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        String wktOut1 = new GeometryEncoderWkt().encode(geometry)
        then:
        geometry instanceof Point
        geometry.getType() == GeometryType.POINT
        geometry.getAxes() == Axes.XYZM
        geometry.isEmpty()
        wktOut1 == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'LINESTRING XY'() {
        given:
        String wkt = "LINESTRING(10.0 10.0,20.0 20.0,30.0 40.0)"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)

        then:
        geometry instanceof LineString
        geometry.getType() == GeometryType.LINE_STRING
        ((LineString) geometry).getValue().getNumPositions() == 3
        ((LineString) geometry).getValue().getAxes() == Axes.XY
        ((LineString) geometry).getValue().getCoordinates() == [10.0, 10.0, 20.0, 20.0, 30.0, 40.0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'LINESTRING XY with EMPTY'() {
        given:
        String wkt = "LINESTRING(10.0           10.0, EMPTY      ,    30.0           40.0)"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        String wktOut1 = new GeometryEncoderWkt().encode(geometry)
        then:
        geometry instanceof LineString
        geometry.getType() == GeometryType.LINE_STRING
        ((LineString) geometry).getValue().getNumPositions() == 2
        ((LineString) geometry).getValue().getAxes() == Axes.XY
        ((LineString) geometry).getValue().getCoordinates() == [10.0, 10.0, 30.0, 40.0] as double[]
        wktOut1 == "LINESTRING(10.0 10.0,30.0 40.0)"
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == "LINESTRING(10.0 10.0,30.0 40.0)"
    }

    def 'LINESTRING XYZ'() {
        given:
        String wkt = "LINESTRING Z(10.0 10.0 1.0,20.0 20.0 2.0)"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry instanceof LineString
        geometry.getType() == GeometryType.LINE_STRING
        ((LineString) geometry).getValue().getNumPositions() == 2
        ((LineString) geometry).getValue().getAxes() == Axes.XYZ
        ((LineString) geometry).getValue().getCoordinates() == [10.0, 10.0, 1.0, 20.0, 20.0, 2.0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'MULTIPOINT XY'() {
        given:
        String wkt = "MULTIPOINT((10.0 10.0),(20.0 20.0))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.MULTI_POINT
        ((MultiPoint) geometry).getNumGeometries() == 2
        ((MultiPoint) geometry).getValue().get(0).getValue() == Position.ofXY(10.0, 10.0)
        ((MultiPoint) geometry).getValue().get(1).getValue() == Position.ofXY(20.0, 20.0)
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'POLYGON XY'() {
        given:
        String wkt = "POLYGON((10.0 10.0,20.0 20.0,30.0 40.0,10.0 10.0))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.POLYGON
        ((Polygon) geometry).getNumRings() == 1
        ((Polygon) geometry).getValue().get(0).getValue().getCoordinates() == [10.0, 10.0, 20.0, 20.0, 30.0, 40.0, 10.0, 10.0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'MULTILINESTRING XY'() {
        given:
        String wkt = "MULTILINESTRING((10.0 10.0,20.0 20.0),(30.0 40.0,50.0 60.0))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.MULTI_LINE_STRING
        ((MultiLineString) geometry).getValue().size() == 2
        ((MultiLineString) geometry).getValue().get(0).getValue().getCoordinates() == [10.0, 10.0, 20.0, 20.0] as double[]
        ((MultiLineString) geometry).getValue().get(1).getValue().getCoordinates() == [30.0, 40.0, 50.0, 60.0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'MULTIPOLYGON XY'() {
        given:
        String wkt = "MULTIPOLYGON(((10.0 10.0,20.0 20.0,30.0 40.0,10.0 10.0)),((50.0 50.0,60.0 60.0,70.0 80.0,50.0 50.0)))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.MULTI_POLYGON
        ((MultiPolygon) geometry).getNumGeometries() == 2
        ((MultiPolygon) geometry).getValue().get(0).getValue().get(0).getValue().getCoordinates() == [10.0, 10.0, 20.0, 20.0, 30.0, 40.0, 10.0, 10.0] as double[]
        ((MultiPolygon) geometry).getValue().get(1).getValue().get(0).getValue().getCoordinates() == [50.0, 50.0, 60.0, 60.0, 70.0, 80.0, 50.0, 50.0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'GEOMETRYCOLLECTION with Point and LineString'() {
        given:
        String wkt = "GEOMETRYCOLLECTION(POINT(10.0 10.0),LINESTRING(20.0 20.0,30.0 30.0))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.GEOMETRY_COLLECTION
        ((GeometryCollection) geometry).getValue().size() == 2
        ((GeometryCollection) geometry).getValue().get(0) instanceof Point
        ((GeometryCollection) geometry).getValue().get(1) instanceof LineString
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'Nested GEOMETRYCOLLECTION with Point and LineString / MultiPoint'() {
        given:
        String wkt = "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(10.0 10.0),LINESTRING(20.0 20.0,30.0 30.0)),MULTIPOINT((10.0 10.0),(10.0 10.0)))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.GEOMETRY_COLLECTION
        ((GeometryCollection) geometry).getValue().size() == 2
        ((GeometryCollection) geometry).getValue().get(0) instanceof GeometryCollection
        ((GeometryCollection) geometry).getValue().get(1) instanceof MultiPoint
        ((GeometryCollection)((GeometryCollection) geometry).getValue().get(0)).getValue().get(0) instanceof Point
        ((GeometryCollection)((GeometryCollection) geometry).getValue().get(0)).getValue().get(1) instanceof LineString
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'POLYHEDRALSURFACE XY'() {
        given:
        String wkt = "POLYHEDRALSURFACE(((0.0 0.0,0.0 1.0,1.0 1.0,0.0 0.0)),((0.0 0.0,1.0 1.0,1.0 0.0,0.0 0.0)))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.POLYHEDRAL_SURFACE
        ((PolyhedralSurface) geometry).getNumPolygons() == 2
        ((PolyhedralSurface) geometry).getValue().get(0).getValue().get(0).getValue().getCoordinates() == [0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'CIRCULARSTRING XY'() {
        given:
        String wkt = "CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0)"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.CIRCULAR_STRING
        ((CircularString) geometry).getValue().getCoordinates() == [0, 0, 1, 1, 2, 0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'COMPOUNDCURVE XY'() {
        given:
        String wkt = "COMPOUNDCURVE((0.0 0.0,1.0 1.0),(1.0 1.0,2.0 0.0))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.COMPOUND_CURVE
        ((CompoundCurve) geometry).getNumGeometries() == 2
        ((CompoundCurve) geometry).getValue().get(0).getType() == GeometryType.LINE_STRING
        ((CompoundCurve) geometry).getValue().get(1).getType() == GeometryType.LINE_STRING
        ((CompoundCurve) geometry).getValue().get(0).getValue().getCoordinates() == [0.0, 0.0, 1.0, 1.0] as double[]
        ((CompoundCurve) geometry).getValue().get(1).getValue().getCoordinates() == [1.0, 1.0, 2.0, 0.0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'CURVEPOLYGON XY'() {
        given:
        String wkt = "CURVEPOLYGON(CIRCULARSTRING(0.0 0.0,1.0 1.0,0.0 2.0,-1.0 -1.0,0.0 0.0))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.CURVE_POLYGON
        ((CurvePolygon) geometry).getNumRings() == 1
        ((CircularString) (((CurvePolygon) geometry).getValue().get(0))).getValue().getCoordinates() == [0.0, 0.0, 1.0, 1.0, 0.0, 2.0, -1.0, -1.0, 0.0, 0.0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'MULTICURVE XY'() {
        given:
        String wkt = "MULTICURVE((0.0 0.0,1.0 1.0),CIRCULARSTRING(1.0 1.0,2.0 0.0,3.0 1.0))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.MULTI_CURVE
        ((MultiCurve) geometry).getNumGeometries() == 2
        ((MultiCurve) geometry).getValue().get(0).getType() == GeometryType.LINE_STRING
        ((MultiCurve) geometry).getValue().get(1).getType() == GeometryType.CIRCULAR_STRING
        ((LineString) ((MultiCurve) geometry).getValue().get(0)).getValue().getCoordinates() == [0.0, 0.0, 1.0, 1.0] as double[]
        ((CircularString) ((MultiCurve) geometry).getValue().get(1)).getValue().getCoordinates() == [1.0, 1.0, 2.0, 0.0, 3.0, 1.0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }

    def 'MULTISURFACE XY'() {
        given:
        String wkt = "MULTISURFACE(((0.0 0.0,0.0 1.0,1.0 1.0,0.0 0.0)),CURVEPOLYGON((1.0 1.0,2.0 2.0,2.0 1.0,1.0 1.0)))"
        when:
        Geometry geometry = new GeometryDecoderWkt().decode(wkt)
        then:
        geometry.getType() == GeometryType.MULTI_SURFACE
        ((MultiSurface) geometry).getNumGeometries() == 2
        ((MultiSurface) geometry).getValue().get(0).getType() == GeometryType.POLYGON
        ((MultiSurface) geometry).getValue().get(1).getType() == GeometryType.CURVE_POLYGON
        ((Polygon) ((MultiSurface) geometry).getValue().get(0)).getValue().get(0).getValue().getCoordinates() == [0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0] as double[]
        ((LineString) ((CurvePolygon) ((MultiSurface) geometry).getValue().get(1)).getValue().get(0)).getValue().getCoordinates() == [1.0, 1.0, 2.0, 2.0, 2.0, 1.0, 1.0, 1.0] as double[]
        new GeometryEncoderWkt().encode(geometry) == wkt
        new GeometryEncoderWkt().encode(new GeometryDecoderWkb().decode(new GeometryEncoderWkb().encode(geometry))) == wkt
    }
}
