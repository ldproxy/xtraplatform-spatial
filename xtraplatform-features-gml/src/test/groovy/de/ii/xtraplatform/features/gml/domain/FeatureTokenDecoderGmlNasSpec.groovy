/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain

import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.FeatureTokenType
import de.ii.xtraplatform.features.domain.ImmutableFeatureQuery
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableSchemaMapping
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.SchemaMapping
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenDecoderSimple
import de.ii.xtraplatform.features.gml.domain.FeatureTokenDecoderGmlInputProfile
import de.ii.xtraplatform.features.gml.domain.ImmutableFeatureTokenDecoderGmlInputProfile
import de.ii.xtraplatform.geometries.domain.Geometry
import de.ii.xtraplatform.geometries.domain.GeometryType
import de.ii.xtraplatform.streams.app.ReactiveRx
import de.ii.xtraplatform.streams.domain.Reactive
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.xml.namespace.QName

/**
 * Drives the SQL-targeted decoder over real ALKIS NAS feature slices. Fixtures under {@code
 * src/test/resources/nas/} are produced from ALKIS data. The schemas are for an SQL
 * feature provider.
 */
class FeatureTokenDecoderGmlNasSpec extends Specification {

    @Shared Reactive reactive
    @Shared Reactive.Runner runner

    static final String ADV_NS = "http://www.adv-online.de/namespaces/adv/gid/7.1"

    static final Map<String, String> NAMESPACES = [
            "adv": ADV_NS,
            "gml": "http://www.opengis.net/gml/3.2",
            "xlink": "http://www.w3.org/1999/xlink",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance"
    ]

    static final EpsgCrs STORAGE_CRS = EpsgCrs.of(25832)

    /**
     * NAS wraps every geometry in {@code <adv:position>}; the geometry property carries
     * {@code position} as its alias and the profile turns on alias-based lookup.
     */
    static final FeatureTokenDecoderGmlInputProfile NAS_PROFILE =
            ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                    .useAlias(true)
                    .build()

    def setupSpec() {
        reactive = new ReactiveRx()
        runner = reactive.runner("test-nas")
    }

    def cleanupSpec() {
        runner.close()
    }

    static FeatureSchema buildSchema(String featureType, GeometryType geomType) {
        def builder = new ImmutableFeatureSchema.Builder()
                .name(featureType.toLowerCase())
                .sourcePath(tablePath(featureType))
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
        if (geomType != null) {
            builder.putProperties2("geo", new ImmutableFeatureSchema.Builder()
                    .sourcePath("geo")
                    .type(SchemaBase.Type.GEOMETRY)
                    .geometryType(geomType)
                    .alias("position"))
        }
        return builder.build()
    }

    static String tablePath(String featureType) {
        switch (featureType) {
            case "AX_Flurstueck": return "/o11001"
            case "AX_Gebaeude": return "/o31001"
            case "AX_LagebezeichnungOhneHausnummer": return "/o12001"
            default: return "/" + featureType.toLowerCase()
        }
    }

    static FeatureTokenDecoderSimple<byte[], FeatureSchema, SchemaMapping, FeatureEventHandlerSimple.ModifiableContext<FeatureSchema, SchemaMapping>> newDecoder(String featureType, GeometryType geomType) {
        def schema = buildSchema(featureType, geomType)
        new FeatureTokenDecoderGml(
                NAMESPACES,
                [new QName(ADV_NS, featureType)],
                schema,
                ImmutableFeatureQuery.builder().type(schema.getName()).build(),
                Map.of(schema.getName(),
                        new ImmutableSchemaMapping.Builder()
                                .targetSchema(schema)
                                .sourcePathTransformer((path, isValue) -> path)
                                .build()),
                STORAGE_CRS,
                Optional.empty(),
                Optional.empty(),
                NAS_PROFILE)
    }

    List<Object> runDecoder(FeatureTokenDecoderSimple decoder, byte[] xml) {
        def stream = Reactive.Source.inputStream(new ByteArrayInputStream(xml))
                .via(decoder)
                .to(Reactive.Sink.reduce([], (list, element) -> { list << element; return list }))
        return stream.on(runner).run().toCompletableFuture().join() as List<Object>
    }

    def 'chunked feed: a buffer split mid-posList number must not lose the trailing coordinate text'() {
        // When the async XML feeder splits a chunk inside the text content of a gml:posList, the
        // trailing CHARACTERS event is delivered after the geometry decoder has paused
        // (EVENT_INCOMPLETE). The outer FeatureTokenDecoderGml main switch buffers that trailing
        // text; FeatureTokenDecoderGml#onEndElement must forward it to continueDecoding instead of
        // passing "". Without that forwarding the coord frame finalises on an odd number of doubles
        // and GeometryDecoderGml#buildSinglePosList's dimension heuristic promotes XY to XYZ.
        given:
        String feature =
            '<adv:AX_Flurstueck xmlns:adv="' + ADV_NS + '" xmlns:gml="http://www.opengis.net/gml/3.2" gml:id="bug">' +
            '<adv:position><gml:LineString>' +
            '<gml:posList>363334.849 5614983.888 363351.009 5615009.083</gml:posList>' +
            '</gml:LineString></adv:position>' +
            '</adv:AX_Flurstueck>'
        byte[] all = feature.getBytes('UTF-8')
        // Locate the first decimal in "363351.009" and split bytes right after the '.0' — places
        // the boundary mid-number ("363351.0" | "09 5615009.083") so aalto-xml has to emit the
        // posList CHARACTERS in two events.
        int boundary = feature.indexOf('363351.0') + '363351.0'.length()
        byte[] chunk1 = java.util.Arrays.copyOfRange(all, 0, boundary)
        byte[] chunk2 = java.util.Arrays.copyOfRange(all, boundary, all.length)

        def decoder = newDecoder("AX_Flurstueck", GeometryType.LINE_STRING)
        def emitted = []
        decoder.init({ emitted << it } as java.util.function.Consumer<Object>)

        when:
        decoder.onPush(chunk1)
        decoder.onPush(chunk2)
        decoder.onComplete()
        def geom = emitted.find { it instanceof Geometry } as Geometry

        then:
        geom != null
        geom.getAxes().toString() == 'XY'
        geom.getValue().getNumPositions() == 2
        geom.getValue().getCoordinates() == [363334.849d, 5614983.888d, 363351.009d, 5615009.083d] as double[]
    }

    def 'minimal AX_Flurstueck: extra xmlns on feature root must not flip geometry to XYZ'() {
        given:
        String feature4 = '<adv:AX_Flurstueck xmlns:wfs="http://www.opengis.net/wfs/2.0" xmlns:adv="' + ADV_NS + '" xmlns:gml="http://www.opengis.net/gml/3.2" xmlns:xlink="http://www.w3.org/1999/xlink" gml:id="DENW36AL10000GLE">' +
            '<adv:position><gml:Surface gml:id="s1"><gml:patches><gml:PolygonPatch><gml:exterior><gml:Ring>' +
            '<gml:curveMember><gml:Curve gml:id="c1"><gml:segments>' +
            '<gml:LineStringSegment><gml:posList>363351.009 5615009.083 363332.428 5615021.012</gml:posList></gml:LineStringSegment>' +
            '</gml:segments></gml:Curve></gml:curveMember>' +
            '<gml:curveMember><gml:Curve gml:id="c2"><gml:segments>' +
            '<gml:LineStringSegment><gml:posList>363332.428 5615021.012 363351.009 5615009.083</gml:posList></gml:LineStringSegment>' +
            '</gml:segments></gml:Curve></gml:curveMember>' +
            '</gml:Ring></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface></adv:position>' +
            '</adv:AX_Flurstueck>'
        String feature6 = '<adv:AX_Flurstueck xmlns:wfs="http://www.opengis.net/wfs/2.0" xmlns:adv="' + ADV_NS + '" xmlns:gml="http://www.opengis.net/gml/3.2" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:fes="http://www.opengis.net/fes/2.0" gml:id="DENW36AL10000GLE">' +
            '<adv:position><gml:Surface gml:id="s1"><gml:patches><gml:PolygonPatch><gml:exterior><gml:Ring>' +
            '<gml:curveMember><gml:Curve gml:id="c1"><gml:segments>' +
            '<gml:LineStringSegment><gml:posList>363351.009 5615009.083 363332.428 5615021.012</gml:posList></gml:LineStringSegment>' +
            '</gml:segments></gml:Curve></gml:curveMember>' +
            '<gml:curveMember><gml:Curve gml:id="c2"><gml:segments>' +
            '<gml:LineStringSegment><gml:posList>363332.428 5615021.012 363351.009 5615009.083</gml:posList></gml:LineStringSegment>' +
            '</gml:segments></gml:Curve></gml:curveMember>' +
            '</gml:Ring></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface></adv:position>' +
            '</adv:AX_Flurstueck>'

        when:
        def tokens4 = runDecoder(newDecoder("AX_Flurstueck", GeometryType.MULTI_POLYGON), feature4.getBytes("UTF-8"))
        def tokens6 = runDecoder(newDecoder("AX_Flurstueck", GeometryType.MULTI_POLYGON), feature6.getBytes("UTF-8"))
        def geom4 = tokens4.find { it instanceof Geometry } as Geometry
        def geom6 = tokens6.find { it instanceof Geometry } as Geometry

        then:
        geom4 != null
        geom6 != null
        geom4.getAxes().toString() == 'XY'
        geom6.getAxes().toString() == 'XY'
    }

    @Unroll
    def 'NAS feature #featureType: gml:id and geometry are routed to the schema source paths'() {
        given:
        def decoder = newDecoder(featureType, expectedGeomType)
        def bytes = new File("src/test/resources/nas/${featureType}.xml").bytes

        when:
        def tokens = runDecoder(decoder, bytes)

        then:
        // Document structure: starts with INPUT, contains exactly one feature.
        tokens.first() == FeatureTokenType.INPUT
        tokens.last() == FeatureTokenType.INPUT_END
        tokens.count { it == FeatureTokenType.FEATURE } == 1
        tokens.count { it == FeatureTokenType.FEATURE_END } == 1

        // Every ALKIS feature in the Bonn data set carries an OID-style id starting with "DENW".
        def idValue = valueAtPath(tokens, ["oid"])
        idValue != null
        ((String) idValue).startsWith("DENW")

        def geomTokens = tokens.findAll { it instanceof Geometry } as List<Geometry>
        geomTokens.size() == (expectedGeomType == null ? 0 : 1)
        if (expectedGeomType != null) {
            assert geomTokens[0].type == expectedGeomType :
                    "expected ${expectedGeomType} for ${featureType} but got ${geomTokens[0].type}"
            assert pathBeforeGeometry(tokens) == ["geo"]
        }

        where:
        featureType                          | expectedGeomType
        'AX_Aufnahmepunkt'                   | null
        'AX_BoeschungKliff'                  | null
        'AX_LagebezeichnungOhneHausnummer'   | null
        'AX_PunktortAU'                      | GeometryType.POINT
        'AX_BesondereFlurstuecksgrenze'      | GeometryType.LINE_STRING
        'AX_Fahrwegachse'                    | GeometryType.LINE_STRING
        'AX_Gebaeude'                        | GeometryType.CURVE_POLYGON
        'AX_Wohnbauflaeche'                  | GeometryType.CURVE_POLYGON
        'AX_Strassenverkehr'                 | GeometryType.CURVE_POLYGON
        'AX_Flurstueck'                      | GeometryType.MULTI_SURFACE
        'AX_Wald'                            | GeometryType.CURVE_POLYGON
        'AX_Turm'                            | GeometryType.CURVE_POLYGON
    }

    def 'wrapped wfs:FeatureCollection root from a NAS-shaped document is rejected'() {
        given:
        def decoder = newDecoder('AX_Aufnahmepunkt', null)
        // Re-wrap the bare feature in adv:AX_Bestandsdatenauszug + wfs:FeatureCollection
        // to mimic what the source NAS files look like; CRUD mode must reject it.
        def bare = new File('src/test/resources/nas/AX_Aufnahmepunkt.xml').text
                .replaceFirst('<\\?xml[^>]*\\?>\\s*', '')
        def wrapped = '<?xml version="1.0" encoding="UTF-8"?>\n' +
                '<adv:AX_Bestandsdatenauszug xmlns:adv="http://www.adv-online.de/namespaces/adv/gid/7.1"' +
                ' xmlns:wfs="http://www.opengis.net/wfs/2.0">' +
                '<adv:enthaelt><wfs:FeatureCollection><wfs:member>' +
                bare +
                '</wfs:member></wfs:FeatureCollection></adv:enthaelt>' +
                '</adv:AX_Bestandsdatenauszug>'

        when:
        runDecoder(decoder, wrapped.getBytes("UTF-8"))

        then:
        def e = thrown(Exception)
        rootCauseMessage(e).contains("Multi-feature ingest is not supported")
    }

    private static Object valueAtPath(List<Object> tokens, List<String> targetPath) {
        for (int i = 0; i < tokens.size() - 2; i++) {
            if (tokens[i] != FeatureTokenType.VALUE) continue
            if (tokens.get(i + 1) == targetPath) {
                return tokens.get(i + 2)
            }
        }
        return null
    }

    private static List<String> pathBeforeGeometry(List<Object> tokens) {
        for (int i = 0; i < tokens.size(); i++) {
            if (tokens[i] instanceof Geometry && i > 0 && tokens[i - 1] instanceof List) {
                return (List<String>) tokens[i - 1]
            }
        }
        return null
    }

    private static String rootCauseMessage(Throwable t) {
        Throwable cur = t
        while (cur.cause != null && cur.cause != cur) cur = cur.cause
        return cur.message ?: ""
    }
}
