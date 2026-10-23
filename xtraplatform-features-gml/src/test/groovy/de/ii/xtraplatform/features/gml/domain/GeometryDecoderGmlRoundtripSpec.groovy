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
import de.ii.xtraplatform.geometries.domain.Geometry
import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryEncoderWkt
import spock.lang.Specification

import javax.xml.stream.XMLOutputFactory

/**
 * Round-trip tests for the NAS-observed GML geometry patterns (see Appendix A of
 * CRUD_GML_PLAN.md). For each pattern the spec:
 *   1. decodes the input GML and asserts the WKT of the decoded geometry,
 *   2. re-encodes the geometry using GeometryEncoderGml with options chosen to match the
 *      input shape (e.g. USE_SURFACE_RING_CURVE for Surface/Ring/Curve inputs),
 *   3. decodes the re-encoded GML and asserts the WKT is identical.
 *
 * The encoder is permitted to produce a structurally different but semantically equivalent
 * GML form, so the round-trip is verified at the geometry level (via WKT) rather than as
 * exact-string equality. Initial fixtures are synthetic NAS-shaped snippets; once the NAS
 * extractor lands, swap in real ALKIS slices from src/test/resources/nas/.
 */
class GeometryDecoderGmlRoundtripSpec extends Specification {

    static final String GML_NS_DECL = ' xmlns:gml="http://www.opengis.net/gml/3.2"'

    static Geometry<?> decodeGml(String xml) {
        // Ensure the gml prefix is bound — the encoder emits unqualified `gml:` elements.
        String input = xml.contains('xmlns:gml=') ? xml : injectGmlNamespace(xml)
        AsyncXMLStreamReader<AsyncByteArrayFeeder> parser = new InputFactoryImpl().createAsyncFor(new byte[0])
        byte[] bytes = input.getBytes("UTF-8")
        parser.getInputFeeder().feedInput(bytes, 0, bytes.length)
        parser.getInputFeeder().endOfInput()
        def decoder = new GeometryDecoderGml()
        Optional<Geometry<?>> g = decoder.decode(parser, Optional.empty(), OptionalInt.empty())
        assert g.isPresent()
        return g.get()
    }

    static String injectGmlNamespace(String xml) {
        int firstClose = xml.indexOf('>')
        int firstSpace = xml.indexOf(' ')
        int insertAt = (firstSpace > 0 && firstSpace < firstClose) ? firstSpace : firstClose
        return xml.substring(0, insertAt) + GML_NS_DECL + xml.substring(insertAt)
    }

    static String wkt(Geometry<?> g) {
        return new GeometryEncoderWkt().encode(g)
    }

    static String encodeGml(Geometry<?> g, Set<GeometryEncoderGml.Options> options) {
        def sw = new StringWriter()
        def xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(sw)
        def encoder = new GeometryEncoderGml(
                xmlWriter, GmlVersion.GML32, options, Optional.of("gml"), Optional.empty(), List.of())
        g.accept(encoder)
        xmlWriter.flush()
        return sw.toString()
    }

    static void roundtrip(String gmlIn, String expectedWkt, Set<GeometryEncoderGml.Options> options) {
        roundtrip(gmlIn, expectedWkt, null, options)
    }

    static void roundtrip(
            String gmlIn,
            String expectedWkt,
            Optional<EpsgCrs> expectedCrs,
            Set<GeometryEncoderGml.Options> options) {
        def geom1 = decodeGml(gmlIn)
        assert wkt(geom1) == expectedWkt
        if (expectedCrs != null) {
            assert geom1.crs == expectedCrs
        }
        String gmlOut = encodeGml(geom1, options)
        def geom2 = decodeGml(gmlOut)
        assert wkt(geom2) == expectedWkt
        if (expectedCrs != null) {
            assert geom2.crs == expectedCrs
        }
    }

    def 'Point (NAS shape)'() {
        expect:
        roundtrip(
                '<gml:Point><gml:pos>365001.5 5621002.25</gml:pos></gml:Point>',
                'POINT(365001.5 5621002.25)',
                Set.of()
        )
    }

    def 'MultiPoint (NAS shape)'() {
        expect:
        roundtrip(
                '''<gml:MultiPoint>
                    <gml:pointMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:pointMember>
                    <gml:pointMember><gml:Point><gml:pos>3 4</gml:pos></gml:Point></gml:pointMember>
                </gml:MultiPoint>''',
                'MULTIPOINT((1.0 2.0),(3.0 4.0))',
                Set.of()
        )
    }

    def 'Curve with one LineStringSegment (NAS shape)'() {
        expect:
        roundtrip(
                '''<gml:Curve>
                    <gml:segments>
                        <gml:LineStringSegment><gml:posList>0 0 1 1 2 0</gml:posList></gml:LineStringSegment>
                    </gml:segments>
                </gml:Curve>''',
                'LINESTRING(0.0 0.0,1.0 1.0,2.0 0.0)',
                Set.of()
        )
    }

    def 'Curve with one Arc (NAS shape)'() {
        expect:
        roundtrip(
                '''<gml:Curve>
                    <gml:segments>
                        <gml:Arc><gml:posList>0 0 1 1 2 0</gml:posList></gml:Arc>
                    </gml:segments>
                </gml:Curve>''',
                'CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0)',
                Set.of()
        )
    }

    def 'Curve with one Circle (NAS shape)'() {
        // gml:Circle decodes to a 5-position closed CIRCULARSTRING (P1, P2, P3, antipode(P2), P1).
        // On re-encode the encoder collapses the same 5 points back to gml:Circle, so the
        // re-decoded geometry has the same WKT.
        expect:
        roundtrip(
                '''<gml:Curve>
                    <gml:segments>
                        <gml:Circle><gml:posList>0 0 1 1 2 0</gml:posList></gml:Circle>
                    </gml:segments>
                </gml:Curve>''',
                'CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0,1.0 -1.0,0.0 0.0)',
                Set.of()
        )
    }

    def 'MultiCurve of linear Curves (NAS shape)'() {
        expect:
        roundtrip(
                '''<gml:MultiCurve>
                    <gml:curveMember>
                        <gml:Curve><gml:segments>
                            <gml:LineStringSegment><gml:posList>0 0 1 1</gml:posList></gml:LineStringSegment>
                        </gml:segments></gml:Curve>
                    </gml:curveMember>
                    <gml:curveMember>
                        <gml:Curve><gml:segments>
                            <gml:LineStringSegment><gml:posList>2 2 3 3</gml:posList></gml:LineStringSegment>
                        </gml:segments></gml:Curve>
                    </gml:curveMember>
                </gml:MultiCurve>''',
                'MULTILINESTRING((0.0 0.0,1.0 1.0),(2.0 2.0,3.0 3.0))',
                Set.of()
        )
    }

    def 'MultiCurve mixing linear and Arc Curves (NAS shape)'() {
        expect:
        roundtrip(
                '''<gml:MultiCurve>
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
                </gml:MultiCurve>''',
                'MULTICURVE((0.0 0.0,1.0 1.0),CIRCULARSTRING(1.0 1.0,2.0 0.0,3.0 1.0))',
                Set.of()
        )
    }

    def 'CompositeCurve of linear Curves (NAS shape)'() {
        expect:
        roundtrip(
                '''<gml:CompositeCurve>
                    <gml:curveMember>
                        <gml:Curve><gml:segments>
                            <gml:LineStringSegment><gml:posList>0 0 1 1</gml:posList></gml:LineStringSegment>
                        </gml:segments></gml:Curve>
                    </gml:curveMember>
                    <gml:curveMember>
                        <gml:Curve><gml:segments>
                            <gml:LineStringSegment><gml:posList>1 1 2 0</gml:posList></gml:LineStringSegment>
                        </gml:segments></gml:Curve>
                    </gml:curveMember>
                </gml:CompositeCurve>''',
                'COMPOUNDCURVE((0.0 0.0,1.0 1.0),(1.0 1.0,2.0 0.0))',
                Set.of(GeometryEncoderGml.Options.USE_SURFACE_RING_CURVE)
        )
    }

    def 'CompositeCurve mixing linear and Arc Curves (NAS shape)'() {
        expect:
        roundtrip(
                '''<gml:CompositeCurve>
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
                </gml:CompositeCurve>''',
                'COMPOUNDCURVE((0.0 0.0,1.0 1.0),CIRCULARSTRING(1.0 1.0,2.0 0.0,3.0 1.0))',
                Set.of(GeometryEncoderGml.Options.USE_SURFACE_RING_CURVE)
        )
    }

    def 'CompositeCurve from NAS (4 primitive curveMembers, one Arc, one merged Curve)'() {
        expect:
        // The last <gml:Curve> contains two <gml:LineStringSegment>s — segments inside a single
        // Curve are merged into one LineString primitive. So this CompositeCurve has 4 primitive
        // curveMembers after decode, not 5.
        //
        // CRS handling: srsName="urn:adv:crs:ETRS89_UTM32" is an ADV URN form. The decoder does
        // not yet resolve these (srsNameMappings support is pending). The current contract is
        // therefore "unresolved srsName → no CRS captured"; this assertion locks that behaviour
        // and will need to flip to Optional.of(EpsgCrs.of(25832)) when srsName mappings land.
        roundtrip(
                '''<gml:CompositeCurve gml:id="o62030.id.27494041.position_complex.Geom_0" srsName="urn:adv:crs:ETRS89_UTM32" srsDimension="2">
                    <gml:curveMember>
                        <gml:Curve gml:id="o62030.id.27494041.position_complex.Geom_1">
                            <gml:segments>
                                <gml:LineStringSegment>
                                    <gml:posList>364511.241 5614723.635 364509.431 5614726.013</gml:posList>
                                </gml:LineStringSegment>
                            </gml:segments>
                        </gml:Curve>
                    </gml:curveMember>
                    <gml:curveMember>
                        <gml:Curve gml:id="o62030.id.27494041.position_complex.Geom_2">
                            <gml:segments>
                                <gml:Arc>
                                    <gml:posList>364509.431 5614726.013 364508.987 5614730.917 364512.731 5614734.111</gml:posList>
                                </gml:Arc>
                            </gml:segments>
                        </gml:Curve>
                    </gml:curveMember>
                    <gml:curveMember>
                        <gml:Curve gml:id="o62030.id.27494041.position_complex.Geom_3">
                            <gml:segments>
                                <gml:LineStringSegment>
                                    <gml:posList>364512.731 5614734.111 364520.919 5614736.949</gml:posList>
                                </gml:LineStringSegment>
                            </gml:segments>
                        </gml:Curve>
                    </gml:curveMember>
                    <gml:curveMember>
                        <gml:Curve gml:id="o62030.id.27494041.position_complex.Geom_4">
                            <gml:segments>
                                <gml:LineStringSegment>
                                    <gml:posList>364520.919 5614736.949 364522.483 5614738.547</gml:posList>
                                </gml:LineStringSegment>
                                <gml:LineStringSegment>
                                    <gml:posList>364522.483 5614738.547 364527.479 5614737.896</gml:posList>
                                </gml:LineStringSegment>
                            </gml:segments>
                        </gml:Curve>
                    </gml:curveMember>
                </gml:CompositeCurve>''',
                'COMPOUNDCURVE((364511.241 5614723.635,364509.431 5614726.013),CIRCULARSTRING(364509.431 5614726.013,364508.987 5614730.917,364512.731 5614734.111),(364512.731 5614734.111,364520.919 5614736.949),(364520.919 5614736.949,364522.483 5614738.547,364527.479 5614737.896))',
                Optional.empty(),
                Set.of(GeometryEncoderGml.Options.USE_SURFACE_RING_CURVE)
        )
    }

    def 'CompositeCurve with resolvable EPSG srsName preserves CRS through round-trip'() {
        expect:
        // Sanity check that the CRS plumbing itself works when the srsName is a form the decoder
        // already recognizes. Uses WITH_SRS_NAME so the encoder emits srsName on its output and
        // the re-decoded geometry can recover the CRS.
        roundtrip(
                '''<gml:CompositeCurve srsName="urn:ogc:def:crs:EPSG::25832">
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
                </gml:CompositeCurve>''',
                'COMPOUNDCURVE((0.0 0.0,1.0 1.0),CIRCULARSTRING(1.0 1.0,2.0 0.0,3.0 1.0))',
                Optional.of(EpsgCrs.of(25832)),
                Set.of(
                        GeometryEncoderGml.Options.USE_SURFACE_RING_CURVE,
                        GeometryEncoderGml.Options.WITH_SRS_NAME)
        )
    }

    def 'Surface with linear PolygonPatch Ring (canonical NAS shape, single curveMember)'() {
        expect:
        roundtrip(
                '''<gml:Surface>
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
                </gml:Surface>''',
                'POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0))',
                Set.of(GeometryEncoderGml.Options.USE_SURFACE_RING_CURVE)
        )
    }

    def 'Surface with PolygonPatch Ring of multiple linear Curves (CurvePolygon)'() {
        expect:
        roundtrip(
                '''<gml:Surface>
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
                </gml:Surface>''',
                'CURVEPOLYGON(COMPOUNDCURVE((0.0 0.0,1.0 0.0),(1.0 0.0,1.0 1.0),(1.0 1.0,0.0 0.0)))',
                Set.of(GeometryEncoderGml.Options.USE_SURFACE_RING_CURVE)
        )
    }

    def 'Surface with PolygonPatch Ring containing an Arc (CurvePolygon)'() {
        expect:
        roundtrip(
                '''<gml:Surface>
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
                </gml:Surface>''',
                'CURVEPOLYGON(CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0,1.0 -1.0,0.0 0.0))',
                Set.of(GeometryEncoderGml.Options.USE_SURFACE_RING_CURVE)
        )
    }

    def 'Surface with PolygonPatch Ring containing a Circle (NAS shape)'() {
        // The Bonn ALKIS data ships round road-intersection polygons whose outer Ring is a single
        // gml:Circle (3 control points). Before circle-aware decoding these tripped the
        // "All rings must be closed" check on Polygon/CurvePolygon construction.
        expect:
        roundtrip(
                '''<gml:Surface>
                    <gml:patches>
                        <gml:PolygonPatch>
                            <gml:exterior>
                                <gml:Ring>
                                    <gml:curveMember>
                                        <gml:Curve><gml:segments>
                                            <gml:Circle><gml:posList>0 0 1 1 2 0</gml:posList></gml:Circle>
                                        </gml:segments></gml:Curve>
                                    </gml:curveMember>
                                </gml:Ring>
                            </gml:exterior>
                        </gml:PolygonPatch>
                    </gml:patches>
                </gml:Surface>''',
                'CURVEPOLYGON(CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0,1.0 -1.0,0.0 0.0))',
                Set.of(GeometryEncoderGml.Options.USE_SURFACE_RING_CURVE)
        )
    }

    def 'Curve with one closed ArcString that is NOT a full circle stays an ArcString'() {
        // 5-position closed CIRCULARSTRING whose four distinct control points are not on a
        // common circle: the encoder must keep emitting gml:ArcString rather than gml:Circle.
        expect:
        roundtrip(
                '''<gml:Curve>
                    <gml:segments>
                        <gml:Arc><gml:posList>0 0 1 1 2 0 1 -3 0 0</gml:posList></gml:Arc>
                    </gml:segments>
                </gml:Curve>''',
                'CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0,1.0 -3.0,0.0 0.0)',
                Set.of()
        )
    }

    def 'MultiSurface of Surfaces (NAS shape) -> MultiPolygon when all linear'() {
        expect:
        roundtrip(
                '''<gml:MultiSurface>
                    <gml:surfaceMember>
                        <gml:Surface><gml:patches><gml:PolygonPatch>
                            <gml:exterior><gml:Ring>
                                <gml:curveMember>
                                    <gml:Curve><gml:segments>
                                        <gml:LineStringSegment><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LineStringSegment>
                                    </gml:segments></gml:Curve>
                                </gml:curveMember>
                            </gml:Ring></gml:exterior>
                        </gml:PolygonPatch></gml:patches></gml:Surface>
                    </gml:surfaceMember>
                    <gml:surfaceMember>
                        <gml:Surface><gml:patches><gml:PolygonPatch>
                            <gml:exterior><gml:Ring>
                                <gml:curveMember>
                                    <gml:Curve><gml:segments>
                                        <gml:LineStringSegment><gml:posList>2 2 3 2 3 3 2 2</gml:posList></gml:LineStringSegment>
                                    </gml:segments></gml:Curve>
                                </gml:curveMember>
                            </gml:Ring></gml:exterior>
                        </gml:PolygonPatch></gml:patches></gml:Surface>
                    </gml:surfaceMember>
                </gml:MultiSurface>''',
                'MULTIPOLYGON(((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0)),((2.0 2.0,3.0 2.0,3.0 3.0,2.0 2.0)))',
                Set.of(GeometryEncoderGml.Options.USE_SURFACE_RING_CURVE)
        )
    }

    def 'MultiSurface with one curved Surface (NAS shape)'() {
        expect:
        roundtrip(
                '''<gml:MultiSurface>
                    <gml:surfaceMember>
                        <gml:Surface><gml:patches><gml:PolygonPatch>
                            <gml:exterior><gml:Ring>
                                <gml:curveMember>
                                    <gml:Curve><gml:segments>
                                        <gml:LineStringSegment><gml:posList>0 0 1 0 1 1 0 0</gml:posList></gml:LineStringSegment>
                                    </gml:segments></gml:Curve>
                                </gml:curveMember>
                            </gml:Ring></gml:exterior>
                        </gml:PolygonPatch></gml:patches></gml:Surface>
                    </gml:surfaceMember>
                    <gml:surfaceMember>
                        <gml:Surface><gml:patches><gml:PolygonPatch>
                            <gml:exterior><gml:Ring>
                                <gml:curveMember>
                                    <gml:Curve><gml:segments>
                                        <gml:Arc><gml:posList>0 0 1 1 2 0 1 -1 0 0</gml:posList></gml:Arc>
                                    </gml:segments></gml:Curve>
                                </gml:curveMember>
                            </gml:Ring></gml:exterior>
                        </gml:PolygonPatch></gml:patches></gml:Surface>
                    </gml:surfaceMember>
                </gml:MultiSurface>''',
                'MULTISURFACE(((0.0 0.0,1.0 0.0,1.0 1.0,0.0 0.0)),CURVEPOLYGON(CIRCULARSTRING(0.0 0.0,1.0 1.0,2.0 0.0,1.0 -1.0,0.0 0.0)))',
                Set.of(GeometryEncoderGml.Options.USE_SURFACE_RING_CURVE)
        )
    }
}
