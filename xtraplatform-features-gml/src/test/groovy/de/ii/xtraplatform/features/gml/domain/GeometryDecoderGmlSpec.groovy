/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain

import com.fasterxml.aalto.AsyncByteArrayFeeder
import com.fasterxml.aalto.AsyncXMLStreamReader
import com.fasterxml.aalto.stax.InputFactoryImpl
import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.geometries.domain.Geometry
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryEncoderWkt
import spock.lang.Specification

class GeometryDecoderGmlSpec extends Specification {

    static Geometry<?> decode(String xml) {
        AsyncXMLStreamReader<AsyncByteArrayFeeder> parser = new InputFactoryImpl().createAsyncFor(new byte[0])
        byte[] bytes = xml.getBytes("UTF-8")
        parser.getInputFeeder().feedInput(bytes, 0, bytes.length)
        parser.getInputFeeder().endOfInput()
        def decoder = new GeometryDecoderGml()
        Optional<Geometry<?>> g = decoder.decode(parser, Optional.empty(), OptionalInt.empty())
        assert g.isPresent()
        return g.get()
    }

    static String wkt(Geometry<?> g) {
        return new GeometryEncoderWkt().encode(g)
    }

    def 'Surface geometry: extra unrelated xmlns declarations on the geometry root must not affect dimensionality'() {
        // Same Surface geometry, wrapped in two different xmlns scopes. Extra namespace declarations
        // on an enclosing element (xsi, fes) must not influence the geometry decoder's axes.
        given:
        String geom = '''<gml:Surface gml:id="s1">
            <gml:patches><gml:PolygonPatch><gml:exterior><gml:Ring>
              <gml:curveMember><gml:Curve gml:id="c1"><gml:segments>
                <gml:LineStringSegment><gml:posList>363351.009 5615009.083 363332.428 5615021.012</gml:posList></gml:LineStringSegment>
              </gml:segments></gml:Curve></gml:curveMember>
              <gml:curveMember><gml:Curve gml:id="c2"><gml:segments>
                <gml:LineStringSegment><gml:posList>363332.428 5615021.012 363351.009 5615009.083</gml:posList></gml:LineStringSegment>
              </gml:segments></gml:Curve></gml:curveMember>
            </gml:Ring></gml:exterior></gml:PolygonPatch></gml:patches>
          </gml:Surface>'''
        String body = '''
            <gml:patches><gml:PolygonPatch><gml:exterior><gml:Ring>
              <gml:curveMember><gml:Curve gml:id="c1"><gml:segments>
                <gml:LineStringSegment><gml:posList>363351.009 5615009.083 363332.428 5615021.012</gml:posList></gml:LineStringSegment>
              </gml:segments></gml:Curve></gml:curveMember>
              <gml:curveMember><gml:Curve gml:id="c2"><gml:segments>
                <gml:LineStringSegment><gml:posList>363332.428 5615021.012 363351.009 5615009.083</gml:posList></gml:LineStringSegment>
              </gml:segments></gml:Curve></gml:curveMember>
            </gml:Ring></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'''
        String onlyGml = '<gml:Surface gml:id="s1" xmlns:gml="http://www.opengis.net/gml/3.2">' + body
        String gmlPlusXsiFes = '<gml:Surface gml:id="s1" xmlns:gml="http://www.opengis.net/gml/3.2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:fes="http://www.opengis.net/fes/2.0">' + body

        when:
        def a = decode(onlyGml)
        def b = decode(gmlPlusXsiFes)

        then:
        a.axes.toString() == 'XY'
        b.axes.toString() == 'XY'
    }

    def 'Point XY'() {
        when:
        def g = decode('<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2"><gml:pos>10 20</gml:pos></gml:Point>')

        then:
        wkt(g) == 'POINT(10.0 20.0)'
    }

    def 'Point preserves srsName'() {
        when:
        def g = decode('<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::25832"><gml:pos>1 2</gml:pos></gml:Point>')

        then:
        wkt(g) == 'POINT(1.0 2.0)'
        g.crs.get() == EpsgCrs.of(25832)
    }

    def 'Point XYZ via srsDimension'() {
        when:
        def g = decode('<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsDimension="3"><gml:pos>1 2 3</gml:pos></gml:Point>')

        then:
        wkt(g) == 'POINT Z(1.0 2.0 3.0)'
    }

    def 'MultiPoint'() {
        when:
        def g = decode('''<gml:MultiPoint xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:pointMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:pointMember>
            <gml:pointMember><gml:Point><gml:pos>3 4</gml:pos></gml:Point></gml:pointMember>
        </gml:MultiPoint>''')

        then:
        wkt(g) == 'MULTIPOINT((1.0 2.0),(3.0 4.0))'
    }

    def 'LineString'() {
        when:
        def g = decode('<gml:LineString xmlns:gml="http://www.opengis.net/gml/3.2"><gml:posList>0 0 1 1 2 0</gml:posList></gml:LineString>')

        then:
        wkt(g) == 'LINESTRING(0.0 0.0,1.0 1.0,2.0 0.0)'
    }

    def 'MultiLineString via lineStringMember'() {
        when:
        def g = decode('''<gml:MultiLineString xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:lineStringMember><gml:LineString><gml:posList>0 0 1 1</gml:posList></gml:LineString></gml:lineStringMember>
            <gml:lineStringMember><gml:LineString><gml:posList>2 2 3 3</gml:posList></gml:LineString></gml:lineStringMember>
        </gml:MultiLineString>''')

        then:
        wkt(g) == 'MULTILINESTRING((0.0 0.0,1.0 1.0),(2.0 2.0,3.0 3.0))'
    }

    def 'MultiCurve of LineStrings collapses to MultiLineString'() {
        when:
        def g = decode('''<gml:MultiCurve xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:curveMember><gml:LineString><gml:posList>0 0 1 1</gml:posList></gml:LineString></gml:curveMember>
            <gml:curveMember><gml:LineString><gml:posList>2 2 3 3</gml:posList></gml:LineString></gml:curveMember>
        </gml:MultiCurve>''')

        then:
        wkt(g) == 'MULTILINESTRING((0.0 0.0,1.0 1.0),(2.0 2.0,3.0 3.0))'
    }

    def 'MultiCurve with a curved member -> MultiCurve'() {
        when:
        def g = decode('''<gml:MultiCurve xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:curveMember><gml:LineString><gml:posList>0 0 1 1</gml:posList></gml:LineString></gml:curveMember>
            <gml:curveMember>
                <gml:Curve><gml:segments><gml:Arc><gml:posList>1 1 2 0 3 1</gml:posList></gml:Arc></gml:segments></gml:Curve>
            </gml:curveMember>
        </gml:MultiCurve>''')

        then:
        wkt(g) == 'MULTICURVE((0.0 0.0,1.0 1.0),CIRCULARSTRING(1.0 1.0,2.0 0.0,3.0 1.0))'
    }

    def 'Curve with single LineStringSegment -> LineString'() {
        when:
        def g = decode('''<gml:Curve xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:segments>
                <gml:LineStringSegment><gml:posList>0 0 1 1 2 0</gml:posList></gml:LineStringSegment>
            </gml:segments>
        </gml:Curve>''')

        then:
        wkt(g) == 'LINESTRING(0.0 0.0,1.0 1.0,2.0 0.0)'
    }

    def 'Curve with Arc segment -> CircularString'() {
        when:
        def g = decode('''<gml:Curve xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:segments>
                <gml:Arc><gml:posList>0 0 1 1 2 0</gml:posList></gml:Arc>
            </gml:segments>
        </gml:Curve>''')

        then:
        wkt(g) == 'CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0)'
    }

    def 'Curve with Circle segment -> 5-position closed CircularString'() {
        // A gml:Circle is geometrically closed; we expand the 3 control points to a 5-position
        // closed CIRCULARSTRING so isClosed() holds and PostGIS accepts it as a full circle.
        when:
        def g = decode('''<gml:Curve xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:segments>
                <gml:Circle><gml:posList>0 0 1 1 2 0</gml:posList></gml:Circle>
            </gml:segments>
        </gml:Curve>''')

        then:
        wkt(g) == 'CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0,1.0 -1.0,0.0 0.0)'
    }

    def 'Curve with multiple LineStringSegments -> merged LineString'() {
        when:
        def g = decode('''<gml:Curve xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:segments>
                <gml:LineStringSegment><gml:posList>0 0 1 1</gml:posList></gml:LineStringSegment>
                <gml:LineStringSegment><gml:posList>1 1 2 0</gml:posList></gml:LineStringSegment>
            </gml:segments>
        </gml:Curve>''')

        then:
        wkt(g) == 'LINESTRING(0.0 0.0,1.0 1.0,2.0 0.0)'
    }

    def 'Curve with multiple Arc segments -> merged CircularString'() {
        when:
        def g = decode('''<gml:Curve xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:segments>
                <gml:Arc><gml:posList>0 0 1 1 2 0</gml:posList></gml:Arc>
                <gml:Arc><gml:posList>2 0 3 -1 4 0</gml:posList></gml:Arc>
            </gml:segments>
        </gml:Curve>''')

        then:
        wkt(g) == 'CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0,3.0 -1.0,4.0 0.0)'
    }

    def 'Curve with LineStringSegment + Arc -> CompoundCurve'() {
        when:
        def g = decode('''<gml:Curve xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:segments>
                <gml:LineStringSegment><gml:posList>0 0 1 1</gml:posList></gml:LineStringSegment>
                <gml:Arc><gml:posList>1 1 2 0 3 1</gml:posList></gml:Arc>
            </gml:segments>
        </gml:Curve>''')

        then:
        wkt(g) == 'COMPOUNDCURVE((0.0 0.0,1.0 1.0),CIRCULARSTRING(1.0 1.0,2.0 0.0,3.0 1.0))'
    }

    def 'CompositeCurve of LineString + Curve(Arc)'() {
        when:
        def g = decode('''<gml:CompositeCurve xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:curveMember><gml:LineString><gml:posList>0 0 1 1</gml:posList></gml:LineString></gml:curveMember>
            <gml:curveMember>
                <gml:Curve><gml:segments><gml:Arc><gml:posList>1 1 2 0 3 1</gml:posList></gml:Arc></gml:segments></gml:Curve>
            </gml:curveMember>
        </gml:CompositeCurve>''')

        then:
        wkt(g) == 'COMPOUNDCURVE((0.0 0.0,1.0 1.0),CIRCULARSTRING(1.0 1.0,2.0 0.0,3.0 1.0))'
    }

    def 'Polygon with LinearRing exterior'() {
        when:
        def g = decode('''<gml:Polygon xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:exterior>
                <gml:LinearRing><gml:posList>0 0 1 0 1 1 0 1 0 0</gml:posList></gml:LinearRing>
            </gml:exterior>
        </gml:Polygon>''')

        then:
        wkt(g) == 'POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))'
    }

    def 'Polygon with exterior + interior LinearRings'() {
        when:
        def g = decode('''<gml:Polygon xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:exterior>
                <gml:LinearRing><gml:posList>0 0 10 0 10 10 0 10 0 0</gml:posList></gml:LinearRing>
            </gml:exterior>
            <gml:interior>
                <gml:LinearRing><gml:posList>2 2 4 2 4 4 2 4 2 2</gml:posList></gml:LinearRing>
            </gml:interior>
        </gml:Polygon>''')

        then:
        wkt(g) == 'POLYGON((0.0 0.0,10.0 0.0,10.0 10.0,0.0 10.0,0.0 0.0),(2.0 2.0,4.0 2.0,4.0 4.0,2.0 4.0,2.0 2.0))'
    }

    def 'GML 2.1 outerBoundaryIs/innerBoundaryIs and coordinates'() {
        when:
        def g = decode('''<gml:Polygon xmlns:gml="http://www.opengis.net/gml/2.1.2">
            <gml:outerBoundaryIs>
                <gml:LinearRing><gml:coordinates>0,0 1,0 1,1 0,1 0,0</gml:coordinates></gml:LinearRing>
            </gml:outerBoundaryIs>
        </gml:Polygon>''')

        then:
        wkt(g) == 'POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 1.0,0.0 0.0))'
    }

    def 'MultiPolygon via polygonMember'() {
        when:
        def g = decode('''<gml:MultiPolygon xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:polygonMember>
                <gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>
            </gml:polygonMember>
            <gml:polygonMember>
                <gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>5 5 6 5 6 6 5 5</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>
            </gml:polygonMember>
        </gml:MultiPolygon>''')

        then:
        wkt(g) == 'MULTIPOLYGON(((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0)),((5.0 5.0,6.0 5.0,6.0 6.0,5.0 5.0)))'
    }

    def 'MultiSurface of Polygons -> MultiPolygon'() {
        when:
        def g = decode('''<gml:MultiSurface xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:surfaceMember>
                <gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>
            </gml:surfaceMember>
        </gml:MultiSurface>''')

        then:
        wkt(g) == 'MULTIPOLYGON(((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0)))'
    }

    def 'MultiSurface with curved member -> MultiSurface'() {
        when:
        def g = decode('''<gml:MultiSurface xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:surfaceMember>
                <gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>
            </gml:surfaceMember>
            <gml:surfaceMember>
                <gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior>
                    <gml:Ring><gml:curveMember>
                        <gml:Curve><gml:segments>
                            <gml:Arc><gml:posList>0 0 1 1 2 0 1 -1 0 0</gml:posList></gml:Arc>
                        </gml:segments></gml:Curve>
                    </gml:curveMember></gml:Ring>
                </gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>
            </gml:surfaceMember>
        </gml:MultiSurface>''')

        then:
        wkt(g) == 'MULTISURFACE(((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0)),CURVEPOLYGON(CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0,1.0 -1.0,0.0 0.0)))'
    }

    def 'Surface with single PolygonPatch (LinearRing) -> Polygon'() {
        when:
        def g = decode('''<gml:Surface xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:patches>
                <gml:PolygonPatch>
                    <gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LinearRing></gml:exterior>
                </gml:PolygonPatch>
            </gml:patches>
        </gml:Surface>''')

        then:
        wkt(g) == 'POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0))'
    }

    def 'Surface with gml:Ring of multiple linear curveMembers -> CurvePolygon (Curves preserved)'() {
        when:
        def g = decode('''<gml:Surface xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:patches>
                <gml:PolygonPatch>
                    <gml:exterior>
                        <gml:Ring>
                            <gml:curveMember>
                                <gml:Curve><gml:segments>
                                    <gml:LineStringSegment><gml:posList>0 0 1 0</gml:posList></gml:LineStringSegment>
                                </gml:segments></gml:Curve>
                            </gml:curveMember>
                            <gml:curveMember>
                                <gml:Curve><gml:segments>
                                    <gml:LineStringSegment><gml:posList>1 0 1 1</gml:posList></gml:LineStringSegment>
                                </gml:segments></gml:Curve>
                            </gml:curveMember>
                            <gml:curveMember>
                                <gml:Curve><gml:segments>
                                    <gml:LineStringSegment><gml:posList>1 1 0 0</gml:posList></gml:LineStringSegment>
                                </gml:segments></gml:Curve>
                            </gml:curveMember>
                        </gml:Ring>
                    </gml:exterior>
                </gml:PolygonPatch>
            </gml:patches>
        </gml:Surface>''')

        then:
        wkt(g) == 'CURVEPOLYGON(COMPOUNDCURVE((0.0 0.0,1.0 0.0),(1.0 0.0,1.0 1.0),(1.0 1.0,0.0 0.0)))'
    }

    def 'Surface with gml:Ring of single linear curveMember -> Polygon'() {
        when:
        def g = decode('''<gml:Surface xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:patches>
                <gml:PolygonPatch>
                    <gml:exterior>
                        <gml:Ring>
                            <gml:curveMember>
                                <gml:Curve><gml:segments>
                                    <gml:LineStringSegment><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LineStringSegment>
                                </gml:segments></gml:Curve>
                            </gml:curveMember>
                        </gml:Ring>
                    </gml:exterior>
                </gml:PolygonPatch>
            </gml:patches>
        </gml:Surface>''')

        then:
        wkt(g) == 'POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0))'
    }

    def 'Surface with gml:Ring containing an Arc -> CurvePolygon'() {
        when:
        def g = decode('''<gml:Surface xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:patches>
                <gml:PolygonPatch>
                    <gml:exterior>
                        <gml:Ring>
                            <gml:curveMember>
                                <gml:Curve><gml:segments>
                                    <gml:Arc><gml:posList>0 0 1 1 2 0 1 -1 0 0</gml:posList></gml:Arc>
                                </gml:segments></gml:Curve>
                            </gml:curveMember>
                        </gml:Ring>
                    </gml:exterior>
                </gml:PolygonPatch>
            </gml:patches>
        </gml:Surface>''')

        then:
        wkt(g) == 'CURVEPOLYGON(CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0,1.0 -1.0,0.0 0.0))'
    }

    def 'Surface with gml:Ring mixing LineStringSegment + Arc -> CurvePolygon with CompoundCurve ring'() {
        when:
        def g = decode('''<gml:Surface xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:patches>
                <gml:PolygonPatch>
                    <gml:exterior>
                        <gml:Ring>
                            <gml:curveMember>
                                <gml:Curve><gml:segments>
                                    <gml:LineStringSegment><gml:posList>0 0 1 1</gml:posList></gml:LineStringSegment>
                                </gml:segments></gml:Curve>
                            </gml:curveMember>
                            <gml:curveMember>
                                <gml:Curve><gml:segments>
                                    <gml:Arc><gml:posList>1 1 2 0 3 1</gml:posList></gml:Arc>
                                </gml:segments></gml:Curve>
                            </gml:curveMember>
                            <gml:curveMember>
                                <gml:Curve><gml:segments>
                                    <gml:LineStringSegment><gml:posList>3 1 0 0</gml:posList></gml:LineStringSegment>
                                </gml:segments></gml:Curve>
                            </gml:curveMember>
                        </gml:Ring>
                    </gml:exterior>
                </gml:PolygonPatch>
            </gml:patches>
        </gml:Surface>''')

        then:
        wkt(g) == 'CURVEPOLYGON(COMPOUNDCURVE((0.0 0.0,1.0 1.0),CIRCULARSTRING(1.0 1.0,2.0 0.0,3.0 1.0),(3.0 1.0,0.0 0.0)))'
    }

    def 'CompositeSurface of Polygons -> PolyhedralSurface (open)'() {
        when:
        def g = decode('''<gml:CompositeSurface xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:surfaceMember>
                <gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>
            </gml:surfaceMember>
            <gml:surfaceMember>
                <gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 0 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>
            </gml:surfaceMember>
        </gml:CompositeSurface>''')

        then:
        wkt(g) == 'POLYHEDRALSURFACE(((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0)),((0.0 0.0,1.0 0.0,0.0 1.0,0.0 0.0)))'
    }

    def 'PolyhedralSurface element -> open PolyhedralSurface'() {
        when:
        def g = decode('''<gml:PolyhedralSurface xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:patches>
                <gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch>
                <gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 0 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch>
            </gml:patches>
        </gml:PolyhedralSurface>''')

        then:
        wkt(g) == 'POLYHEDRALSURFACE(((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0)),((0.0 0.0,1.0 0.0,0.0 1.0,0.0 0.0)))'
    }

    def 'Solid with outer shell -> closed PolyhedralSurface'() {
        when:
        def g = decode('''<gml:Solid xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:exterior>
                <gml:Shell>
                    <gml:surfaceMember>
                        <gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>
                    </gml:surfaceMember>
                    <gml:surfaceMember>
                        <gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 0 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>
                    </gml:surfaceMember>
                </gml:Shell>
            </gml:exterior>
        </gml:Solid>''')

        then:
        g.closed
        wkt(g) == 'POLYHEDRALSURFACE(((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0)),((0.0 0.0,1.0 0.0,0.0 1.0,0.0 0.0)))'
    }

    def 'MultiGeometry -> GeometryCollection'() {
        when:
        def g = decode('''<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:geometryMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:geometryMember>
            <gml:geometryMember><gml:LineString><gml:posList>0 0 1 1</gml:posList></gml:LineString></gml:geometryMember>
        </gml:MultiGeometry>''')

        then:
        wkt(g) == 'GEOMETRYCOLLECTION(POINT(1.0 2.0),LINESTRING(0.0 0.0,1.0 1.0))'
    }

    def 'LineString XYZ via srsDimension on LineString'() {
        when:
        def g = decode('<gml:LineString xmlns:gml="http://www.opengis.net/gml/3.2" srsDimension="3"><gml:posList>0 0 0 1 1 1 2 0 0</gml:posList></gml:LineString>')

        then:
        wkt(g) == 'LINESTRING Z(0.0 0.0 0.0,1.0 1.0 1.0,2.0 0.0 0.0)'
    }

    def 'Solid with inner shell rejected'() {
        when:
        decode('''<gml:Solid xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:exterior>
                <gml:Shell>
                    <gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember>
                </gml:Shell>
            </gml:exterior>
            <gml:interior>
                <gml:Shell>
                    <gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember>
                </gml:Shell>
            </gml:interior>
        </gml:Solid>''')

        then:
        thrown(IOException)
    }

    def 'Unsupported curve segment GeodesicString rejected'() {
        when:
        decode('''<gml:Curve xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:segments>
                <gml:GeodesicString><gml:posList>0 0 1 1</gml:posList></gml:GeodesicString>
            </gml:segments>
        </gml:Curve>''')

        then:
        thrown(IOException)
    }

    def 'Unsupported surface patch Triangle rejected'() {
        when:
        decode('''<gml:Surface xmlns:gml="http://www.opengis.net/gml/3.2">
            <gml:patches>
                <gml:Triangle><gml:exterior><gml:LinearRing><gml:posList>0 0 1 0 0 1 0 0</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle>
            </gml:patches>
        </gml:Surface>''')

        then:
        thrown(IOException)
    }

    def 'CompositeSolid rejected'() {
        when:
        decode('<gml:CompositeSolid xmlns:gml="http://www.opengis.net/gml/3.2"></gml:CompositeSolid>')

        then:
        thrown(IOException)
    }

    def 'MultiSolid rejected'() {
        when:
        decode('<gml:MultiSolid xmlns:gml="http://www.opengis.net/gml/3.2"></gml:MultiSolid>')

        then:
        thrown(IOException)
    }

    def 'OrientableCurve rejected'() {
        when:
        decode('<gml:OrientableCurve xmlns:gml="http://www.opengis.net/gml/3.2"></gml:OrientableCurve>')

        then:
        thrown(IOException)
    }

    def 'OrientableSurface rejected'() {
        when:
        decode('<gml:OrientableSurface xmlns:gml="http://www.opengis.net/gml/3.2"></gml:OrientableSurface>')

        then:
        thrown(IOException)
    }
}
