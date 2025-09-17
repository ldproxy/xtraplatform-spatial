/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.util.TokenBuffer
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.geometries.domain.transcode.json.GeometryDecoderJson
import de.ii.xtraplatform.geometries.domain.transcode.json.GeometryEncoderJson
import spock.lang.Specification

class GeometryJsonSpec extends Specification {

    ObjectMapper objectMapper = new ObjectMapper()

    def 'POINT XY'() {

        given:
        Geometry<?> geometry = Point.of(10.81, 10.37)
        String jsonString = "{\"type\":\"Point\",\"coordinates\":[10.81,10.37]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false);
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString);
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof Point
        Point point = geometry2 as Point
        point.getAxes() == Axes.XY
        point.getValue().getCoordinates()[0] == Double.valueOf(10.81)
        point.getValue().getCoordinates()[1] == Double.valueOf(10.37)
    }

    def 'POINT XY in CRS84'() {

        given:
        Geometry<?> geometry = Point.of(10.81, 10.37, OgcCrs.CRS84)
        String jsonString = "{\"type\":\"Point\",\"coordinates\":[10.81,10.37]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false);
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString);
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.of(OgcCrs.CRS84), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof Point
        Point point = geometry2 as Point
        point.getAxes() == Axes.XY
        point.getValue().getCoordinates()[0] == Double.valueOf(10.81)
        point.getValue().getCoordinates()[1] == Double.valueOf(10.37)
    }

    def 'POINT XYZ in CRS84h'() {
        given:
        Geometry<?> geometry = Point.of(Position.ofXYZ(10.81, 10.37, 5.00), Optional.of(OgcCrs.CRS84h))
        String jsonString = "{\"type\":\"Point\",\"coordinates\":[10.81,10.37,5.0]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.of(OgcCrs.CRS84h), Optional.of(Axes.XYZ))

        then:
        jsonOut == jsonString
        geometry2 instanceof Point
        Point point = geometry2 as Point
        point.getAxes() == Axes.XYZ
        point.getValue().getCoordinates() == [10.81, 10.37, 5.0] as double[]
    }

    def 'POINT XYM'() {
        given:
        Geometry<?> geometry = Point.of(Position.ofXYM(10.81, 10.37, 5.00), Optional.of(OgcCrs.CRS84))

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false);
         geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)

        then:
        jsonOut == "{\"type\":\"Point\",\"coordinates\":[10.81,10.37,5.0]}"
    }

    def 'POINT XYZM'() {
        given:
        Geometry<?> geometry = Point.of(Position.ofXYZM(10.81, 10.37, 5.00, 7.50), Optional.of(OgcCrs.CRS84h))

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false);
         geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)

        then:
        jsonOut == "{\"type\":\"Point\",\"coordinates\":[10.81,10.37,5.0,7.5]}"
    }

    def 'POINT XY EMPTY'() {
        given:
        Geometry<?> geometry = Point.empty(Axes.XY);

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false);
         geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)

        then:
        jsonOut == "{\"type\":\"Point\",\"coordinates\":[]}"
    }

    def 'POINT XYZ EMPTY'() {
        given:
        Geometry<?> geometry = Point.empty(Axes.XYZ);

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false);
         geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)

        then:
        jsonOut == "{\"type\":\"Point\",\"coordinates\":[]}"
    }

    def 'LINESTRING XY'() {
        given:
        Geometry<?> geometry = LineString.of(new double[]{10.0,10.0,20.0,20.0,30.0,40.0}, OgcCrs.CRS84);
        String jsonString = "{\"type\":\"LineString\",\"coordinates\":[[10.0,10.0],[20.0,20.0],[30.0,40.0]]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false);
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString);
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.of(OgcCrs.CRS84), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof LineString
        LineString line = geometry2 as LineString
        line.getAxes() == Axes.XY
        line.getValue().getCoordinates() == [10.0,10.0,20.0,20.0,30.0,40.0] as double[]
    }

    def 'LINESTRING XYZ'() {
        given:
        Geometry<?> geometry = LineString.of(PositionList.of(Axes.XYZ, new double[]{10.0,10.0,1.0,20.0,20.0,2.0,30.0,40.0,3.0}))
        String jsonString = "{\"type\":\"LineString\",\"coordinates\":[[10.0,10.0,1.0],[20.0,20.0,2.0],[30.0,40.0,3.0]]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XYZ))

        then:
        jsonOut == jsonString
        geometry2 instanceof LineString
        LineString line = geometry2 as LineString
        line.getAxes() == Axes.XYZ
        line.getValue().getCoordinates() == [10.0,10.0,1.0,20.0,20.0,2.0,30.0,40.0,3.0] as double[]
    }

    def 'MULTIPOINT XY'() {
        given:
        Geometry<?> geometry = MultiPoint.of(List.of(Point.of(10,10),Point.of(20,20)))
        String jsonString = "{\"type\":\"MultiPoint\",\"coordinates\":[[10.0,10.0],[20.0,20.0]]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof MultiPoint
        MultiPoint multiPoint = geometry2 as MultiPoint
        multiPoint.getAxes() == Axes.XY
        multiPoint.getValue().size() == 2
        multiPoint.getValue()[0].getValue().getCoordinates() == [10.0, 10.0] as double[]
        multiPoint.getValue()[1].getValue().getCoordinates() == [20.0, 20.0] as double[]
    }

    def 'POLYGON XY'() {
        given:
        Geometry<?> geometry = Polygon.of(List.of(PositionList.of(Axes.XY,new double[]{10.0,10.0,20.0,20.0,30.0,40.0,10.0,10.0})))
        String jsonString = "{\"type\":\"Polygon\",\"coordinates\":[[[10.0,10.0],[20.0,20.0],[30.0,40.0],[10.0,10.0]]]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof Polygon
        Polygon polygon = geometry2 as Polygon
        polygon.getAxes() == Axes.XY
        polygon.getValue().get(0).getValue().getCoordinates() == [10.0,10.0,20.0,20.0,30.0,40.0,10.0,10.0] as double[]
    }

    def 'MULTILINESTRING XY'() {
        given:
        Geometry<?> geometry = MultiLineString.of(List.of(
            LineString.of(new double[]{10.0,10.0,20.0,20.0}),
            LineString.of(new double[]{30.0,40.0,50.0,60.0})
        ))
        String jsonString = "{\"type\":\"MultiLineString\",\"coordinates\":[[[10.0,10.0],[20.0,20.0]],[[30.0,40.0],[50.0,60.0]]]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof MultiLineString
        MultiLineString multiLine = geometry2 as MultiLineString
        multiLine.getAxes() == Axes.XY
        multiLine.getValue().size() == 2
        multiLine.getValue()[0].getValue().getCoordinates() == [10.0,10.0,20.0,20.0] as double[]
        multiLine.getValue()[1].getValue().getCoordinates() == [30.0,40.0,50.0,60.0] as double[]
    }

    def 'MULTIPOLYGON XY'() {
        given:
        Geometry<?> geometry = MultiPolygon.of(List.of(
            Polygon.of(List.of(PositionList.of(Axes.XY,new double[]{10.0,10.0,20.0,20.0,30.0,40.0,10.0,10.0}))),
            Polygon.of(List.of(PositionList.of(Axes.XY,new double[]{50.0,50.0,60.0,60.0,70.0,80.0,50.0,50.0})))
        ))
        String jsonString = "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[10.0,10.0],[20.0,20.0],[30.0,40.0],[10.0,10.0]]],[[[50.0,50.0],[60.0,60.0],[70.0,80.0],[50.0,50.0]]]]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof MultiPolygon
        MultiPolygon multiPolygon = geometry2 as MultiPolygon
        multiPolygon.getAxes() == Axes.XY
        multiPolygon.getValue().size() == 2
        multiPolygon.getValue()[0].getValue().get(0).getValue().getCoordinates() == [10.0,10.0,20.0,20.0,30.0,40.0,10.0,10.0] as double[]
        multiPolygon.getValue()[1].getValue().get(0).getValue().getCoordinates() == [50.0,50.0,60.0,60.0,70.0,80.0,50.0,50.0] as double[]
    }

    def 'GEOMETRYCOLLECTION with Point and LineString'() {
        given:
        Geometry<?> geometry = GeometryCollection.of(List.of(
            Point.of(10,10),
            LineString.of(new double[]{20.0,20.0,30.0,30.0})
        ))
        String jsonString = "{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10.0,10.0]},{\"type\":\"LineString\",\"coordinates\":[[20.0,20.0],[30.0,30.0]]}]}"
        String jsonString2 = "{\"geometries\":[{\"coordinates\":[10.0,10.0],\"type\":\"Point\"},{\"coordinates\":[[20.0,20.0],[30.0,30.0]],\"type\":\"LineString\"}],\"type\":\"GeometryCollection\"}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))
        parser = objectMapper.createParser(jsonString2)
        Geometry<?> geometry3 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof GeometryCollection
        GeometryCollection collection = geometry2 as GeometryCollection
        collection.getValue().size() == 2
        collection.getValue()[0] instanceof Point
        collection.getValue()[1] instanceof LineString
        geometry3 instanceof GeometryCollection
        GeometryCollection collection2 = geometry3 as GeometryCollection
        collection2.getValue().size() == 2
        collection2.getValue()[0] instanceof Point
        ((Point)collection2.getValue()[0]).getValue().getCoordinates() == [10.0, 10.0] as double[]
        collection2.getValue()[1] instanceof LineString
        ((LineString)collection2.getValue()[1]).getValue().getCoordinates() == [20.0, 20.0, 30.0, 30.0] as double[]
    }

    def 'CIRCULARSTRING XY'() {
        given:
        Geometry<?> geometry = CircularString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 2.0, 0.0}))
        String jsonString = "{\"type\":\"CircularString\",\"coordinates\":[[0.0,0.0],[1.0,1.0],[2.0,0.0]]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof CircularString
        CircularString cs = geometry2 as CircularString
        cs.getAxes() == Axes.XY
        cs.getValue().getCoordinates() == [0.0,0.0,1.0,1.0,2.0,0.0] as double[]
    }

    def 'Nested GEOMETRYCOLLECTION with Point and LineString / MultiPoint'() {
        given:
        Geometry<?> multiPoint = MultiPoint.of(List.of(Point.of(10,10),Point.of(20,20)))
        Geometry<?> geometryCollection = GeometryCollection.of(List.of(Point.of(10,10),LineString.of(new double[]{20.0,20.0,30.0,30.0})))
        Geometry<?> geometry = GeometryCollection.of(List.of(geometryCollection, multiPoint))

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonOut)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == "{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Point\",\"coordinates\":[10.0,10.0]},{\"type\":\"LineString\",\"coordinates\":[[20.0,20.0],[30.0,30.0]]}]},{\"type\":\"MultiPoint\",\"coordinates\":[[10.0,10.0],[20.0,20.0]]}]}"
        geometry2 instanceof GeometryCollection
        GeometryCollection outerCollection = geometry2 as GeometryCollection
        outerCollection.getValue().size() == 2
        outerCollection.getValue()[0] instanceof GeometryCollection
        GeometryCollection innerCollection = outerCollection.getValue()[0] as GeometryCollection
        innerCollection.getValue().size() == 2
        innerCollection.getValue()[0] instanceof Point
        innerCollection.getValue()[1] instanceof LineString
        outerCollection.getValue()[1] instanceof MultiPoint
        MultiPoint mp = outerCollection.getValue()[1] as MultiPoint
        mp.getValue().size() == 2
        mp.getValue()[0].getValue().getCoordinates() == [10.0, 10.0] as double[]
        mp.getValue()[1].getValue().getCoordinates() == [20.0, 20.0] as double[]
    }

    def 'POLYHEDRALSURFACE XY'() {
        given:
        Geometry geometry = PolyhedralSurface.of(List.of(
            Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0}))),
            Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0})))
        ))
        String jsonString = "{\"type\":\"MultiPolygon\",\"coordinates\":[[[[0.0,0.0],[0.0,1.0],[1.0,1.0],[0.0,0.0]]],[[[0.0,0.0],[1.0,1.0],[1.0,0.0],[0.0,0.0]]]]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof MultiPolygon
        MultiPolygon mp = geometry2 as MultiPolygon
        mp.getAxes() == Axes.XY
        mp.getValue().size() == 2
        mp.getValue()[0].getValue().get(0).getValue().getCoordinates() == [0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0] as double[]
        mp.getValue()[1].getValue().get(0).getValue().getCoordinates() == [0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0] as double[]
    }

    def 'COMPOUNDCURVE XY'() {
        given:
        Geometry<?> geometry = CompoundCurve.of(List.of(
            LineString.of(new double[]{0.0, 0.0, 1.0, 1.0}),
            LineString.of(new double[]{1.0, 1.0, 2.0, 0.0})
        ))
        String jsonString = "{\"type\":\"CompoundCurve\",\"geometries\":[{\"type\":\"LineString\",\"coordinates\":[[0.0,0.0],[1.0,1.0]]},{\"type\":\"LineString\",\"coordinates\":[[1.0,1.0],[2.0,0.0]]}]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof CompoundCurve
        CompoundCurve cc = geometry2 as CompoundCurve
        cc.getAxes() == Axes.XY
        cc.getValue().size() == 2
        cc.getValue()[0] instanceof LineString
        cc.getValue()[1] instanceof LineString
        cc.getValue()[0].getValue().getCoordinates() == [0.0, 0.0, 1.0, 1.0] as double[]
        cc.getValue()[1].getValue().getCoordinates() == [1.0, 1.0, 2.0, 0.0] as double[]
    }

    def 'CURVEPOLYGON XY'() {
        given:
        Geometry<?> geometry = CurvePolygon.of(List.of(
            CircularString.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 1.0, 1.0, 0.0, 2.0, -1.0, -1.0, 0.0, 0.0}))
        ))
        String jsonString = "{\"type\":\"CurvePolygon\",\"geometries\":[{\"type\":\"CircularString\",\"coordinates\":[[0.0,0.0],[1.0,1.0],[0.0,2.0],[-1.0,-1.0],[0.0,0.0]]}]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof CurvePolygon
        CurvePolygon cp = geometry2 as CurvePolygon
        cp.getAxes() == Axes.XY
        cp.getValue().size() == 1
        cp.getValue()[0] instanceof CircularString
        cp.getValue()[0].getValue().getCoordinates() == [0.0, 0.0, 1.0, 1.0, 0.0, 2.0, -1.0, -1.0, 0.0, 0.0] as double[]
    }

    def 'MULTICURVE XY'() {
        given:
        Geometry<?> geometry = MultiCurve.of(List.of(
            LineString.of(new double[]{0.0, 0.0, 1.0, 1.0}),
            CircularString.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 0.0, 3.0, 1.0}))
        ))
        String jsonString = "{\"type\":\"MultiCurve\",\"geometries\":[{\"type\":\"LineString\",\"coordinates\":[[0.0,0.0],[1.0,1.0]]},{\"type\":\"CircularString\",\"coordinates\":[[1.0,1.0],[2.0,0.0],[3.0,1.0]]}]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof MultiCurve
        MultiCurve mc = geometry2 as MultiCurve
        mc.getAxes() == Axes.XY
        mc.getValue().size() == 2
        mc.getValue()[0] instanceof LineString
        mc.getValue()[1] instanceof CircularString
        mc.getValue()[0].getValue().getCoordinates() == [0.0, 0.0, 1.0, 1.0] as double[]
        mc.getValue()[1].getValue().getCoordinates() == [1.0, 1.0, 2.0, 0.0, 3.0, 1.0] as double[]
    }

    def 'MULTISURFACE XY'() {
        given:
        Geometry<?> geometry = MultiSurface.of(List.of(
            Polygon.of(List.of(PositionList.of(Axes.XY, new double[]{0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0}))),
            CurvePolygon.of(List.of(CircularString.of(PositionList.of(Axes.XY, new double[]{1.0, 1.0, 2.0, 2.0, 3, 2, 2.0, 1.0, 1.0, 1.0}))))
        ))
        String jsonString = "{\"type\":\"MultiSurface\",\"geometries\":[{\"type\":\"Polygon\",\"coordinates\":[[[0.0,0.0],[0.0,1.0],[1.0,1.0],[0.0,0.0]]]},{\"type\":\"CurvePolygon\",\"geometries\":[{\"type\":\"CircularString\",\"coordinates\":[[1.0,1.0],[2.0,2.0],[3.0,2.0],[2.0,1.0],[1.0,1.0]]}]}]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XY))

        then:
        jsonOut == jsonString
        geometry2 instanceof MultiSurface
        MultiSurface ms = geometry2 as MultiSurface
        ms.getAxes() == Axes.XY
        ms.getValue().size() == 2
        ms.getValue()[0] instanceof Polygon
        ms.getValue()[1] instanceof CurvePolygon
        ((Polygon)ms.getValue()[0]).getValue().get(0).getValue().getCoordinates() == [0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 0.0] as double[]
        ((CircularString)((CurvePolygon)ms.getValue()[1]).getValue()[0]).getValue().getCoordinates() == [1.0, 1.0, 2.0, 2.0, 3.0, 2.0, 2.0, 1.0, 1.0, 1.0] as double[]
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
        String jsonString = "{\"type\":\"Polyhedron\",\"coordinates\":[[[[[280414.631,5660090.756,40.255],[280414.631,5660088.454,40.255],[280414.631,5660088.454,32.967],[280414.631,5660090.756,32.967],[280414.631,5660090.756,40.255]]],[[[280414.631,5660088.454,40.255],[280405.623,5660088.454,33.256],[280405.623,5660088.454,32.967],[280414.631,5660088.454,32.967],[280414.631,5660088.454,40.255]]],[[[280405.623,5660088.454,33.256],[280405.623,5660090.756,33.256],[280405.623,5660090.756,32.967],[280405.623,5660088.454,32.967],[280405.623,5660088.454,33.256]]],[[[280405.623,5660090.756,33.256],[280414.631,5660090.756,40.255],[280414.631,5660090.756,32.967],[280405.623,5660090.756,32.967],[280405.623,5660090.756,33.256]]],[[[280405.623,5660088.454,33.256],[280414.631,5660088.454,40.255],[280411.722,5660088.454,41.63],[280405.623,5660088.454,33.256]]],[[[280414.631,5660090.756,40.255],[280405.623,5660090.756,33.256],[280411.722,5660090.756,41.63],[280414.631,5660090.756,40.255]]],[[[280414.631,5660088.454,40.255],[280414.631,5660090.756,40.255],[280411.722,5660090.756,41.63],[280411.722,5660088.454,41.63],[280414.631,5660088.454,40.255]]],[[[280405.623,5660090.756,33.256],[280405.623,5660088.454,33.256],[280411.722,5660088.454,41.63],[280411.722,5660090.756,41.63],[280405.623,5660090.756,33.256]]],[[[280414.631,5660090.756,32.967],[280414.631,5660088.454,32.967],[280405.623,5660088.454,32.967],[280405.623,5660090.756,32.967],[280414.631,5660090.756,32.967]]]]]}"

        when:
        TokenBuffer json = new TokenBuffer(objectMapper, false)
        geometry.accept(new GeometryEncoderJson(json))
        String jsonOut = objectMapper.writeValueAsString(json)
        JsonParser parser = objectMapper.createParser(jsonString)
        Geometry<?> geometry2 = new GeometryDecoderJson().decode(parser, Optional.empty(), Optional.of(Axes.XYZ))

        then:
        jsonOut == jsonString
        geometry2 instanceof PolyhedralSurface
        PolyhedralSurface ps = geometry2 as PolyhedralSurface
        ps.getAxes() == Axes.XYZ
        ps.getValue().size() == 9
        ps.isClosed()
    }
}
