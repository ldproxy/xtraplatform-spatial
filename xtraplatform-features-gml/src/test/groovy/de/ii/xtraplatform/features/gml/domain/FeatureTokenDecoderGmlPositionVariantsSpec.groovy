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
import de.ii.xtraplatform.features.domain.ImmutableCrsVariants
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.SchemaMapping
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenDecoderSimple
import de.ii.xtraplatform.geometries.domain.Geometry
import de.ii.xtraplatform.geometries.domain.GeometryType
import de.ii.xtraplatform.geometries.domain.Point
import spock.lang.Specification

import javax.xml.namespace.QName

/**
 * Position-variant routing (see {@link GmlGeometryVariants}), driven entirely by the schema:
 * positions are routed by their verbatim srsName to the variant property that lists the srsName in
 * its originalCrsIdentifiers (with the property's falseEastingDifference applied), 1D positions to
 * the vertical property, and the srsName itself to the crsProperty. Positions without a routed
 * srsName take the normal path.
 */
class FeatureTokenDecoderGmlPositionVariantsSpec extends Specification {

    static final String ADV_NS = "http://www.adv-online.de/namespaces/adv/gid/7.1"
    static final String GK3_HE100 = "urn:adv:crs:DE_DHDN_3GK3_HE100"
    static final String DHHN92 = "urn:adv:crs:DE_DHHN92_NH"

    static final Map<String, String> NAMESPACES = [
            "adv": ADV_NS,
            "gml": "http://www.opengis.net/gml/3.2"
    ]

    static FeatureSchema schema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_punktortau")
                .sourcePath("/o14003")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("pos_srs", new ImmutableFeatureSchema.Builder()
                        .sourcePath("position_srs")
                        .type(SchemaBase.Type.STRING))
                .putProperties2("pos_gk3", new ImmutableFeatureSchema.Builder()
                        .sourcePath("position_gk3")
                        .type(SchemaBase.Type.GEOMETRY)
                        .geometryType(GeometryType.POINT)
                        .nativeCrs(EpsgCrs.of(5677))
                        .addOriginalCrsIdentifiers(GK3_HE100)
                        .falseEastingDifference(3000000d))
                .putProperties2("pos_h", new ImmutableFeatureSchema.Builder()
                        .sourcePath("position_h")
                        .type(SchemaBase.Type.FLOAT)
                        .addOriginalCrsIdentifiers(DHHN92))
                .putProperties2("geo", new ImmutableFeatureSchema.Builder()
                        .sourcePath("position")
                        .type(SchemaBase.Type.GEOMETRY)
                        .geometryType(GeometryType.POINT)
                        .alias("position")
                        .crsVariants(new ImmutableCrsVariants.Builder()
                                .crsProperty("pos_srs")
                                .verticalProperty("pos_h")
                                .addGeometryProperties("pos_gk3")
                                .build()))
                .build()
    }

    static FeatureTokenDecoderGmlInputProfile profile() {
        ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .build()
    }

    static FeatureTokenDecoderSimple<byte[], FeatureSchema, SchemaMapping, FeatureEventHandlerSimple.ModifiableContext<FeatureSchema, SchemaMapping>> newDecoder() {
        def schema = schema()
        new FeatureTokenDecoderGml(
                NAMESPACES,
                [new QName(ADV_NS, "AX_PunktortAU")],
                schema,
                ImmutableFeatureQuery.builder().type(schema.getName()).build(),
                Map.of(schema.getName(),
                        new ImmutableSchemaMapping.Builder()
                                .targetSchema(schema)
                                .sourcePathTransformer((path, isValue) -> path)
                                .build()),
                EpsgCrs.of(25832),
                Optional.empty(),
                Optional.empty(),
                profile())
    }

    List<Object> runDecoder(byte[] xml) {
        def decoder = newDecoder()
        def tokens = []
        decoder.init({ tokens << it } as java.util.function.Consumer<Object>)
        decoder.onPush(xml)
        decoder.onComplete()
        return tokens
    }

    static String feature(String position) {
        """<adv:AX_PunktortAU xmlns:adv="${ADV_NS}"
            xmlns:gml="http://www.opengis.net/gml/3.2"
            gml:id="TESTPUNKTORT0001">
          <adv:position>${position}</adv:position>
        </adv:AX_PunktortAU>"""
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
        for (int i = 1; i < tokens.size(); i++) {
            if (tokens[i] instanceof Geometry && tokens[i - 1] instanceof List) {
                return (List<String>) tokens[i - 1]
            }
        }
        return null
    }

    def 'a foreign-CRS position is routed to the variant property with the false-easting shift'() {
        given:
        def xml = feature("""<gml:Point srsName="${GK3_HE100}" gml:id="P1">
              <gml:pos>446104.620 5551059.770</gml:pos></gml:Point>""")

        when:
        def tokens = runDecoder(xml.getBytes("UTF-8"))
        def geometry = tokens.find { it instanceof Geometry } as Geometry

        then:
        pathBeforeGeometry(tokens) == ["pos_gk3"]
        geometry.getCrs() == Optional.of(EpsgCrs.of(5677))
        (geometry as Point).getValue().getCoordinates() == [3446104.62d, 5551059.77d] as double[]
        valueAtPath(tokens, ["pos_srs"]) == GK3_HE100
    }

    def 'a 1D position is captured as a scalar at the vertical property, no geometry is built'() {
        given:
        def xml = feature("""<gml:Point srsName="${DHHN92}" srsDimension="1" gml:id="P1">
              <gml:pos>229.940</gml:pos></gml:Point>""")

        when:
        def tokens = runDecoder(xml.getBytes("UTF-8"))

        then:
        tokens.count { it instanceof Geometry } == 0
        valueAtPath(tokens, ["pos_h"]) == "229.940"
        valueAtPath(tokens, ["pos_srs"]) == DHHN92
    }

    def 'a position without srsName takes the normal path'() {
        given:
        def xml = feature("""<gml:Point gml:id="P1">
              <gml:pos>448733.315 5539621.758</gml:pos></gml:Point>""")

        when:
        def tokens = runDecoder(xml.getBytes("UTF-8"))
        def geometry = tokens.find { it instanceof Geometry } as Geometry

        then:
        pathBeforeGeometry(tokens) == ["geo"]
        (geometry as Point).getValue().getCoordinates() == [448733.315d, 5539621.758d] as double[]
        valueAtPath(tokens, ["pos_srs"]) == null
    }
}
