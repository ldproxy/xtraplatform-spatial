/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger as LogbackLogger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.FeatureTokenType
import de.ii.xtraplatform.features.domain.ImmutableFeatureQuery
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableSchemaConstraints
import de.ii.xtraplatform.features.domain.ImmutableSchemaMapping
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.SchemaMapping
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenDecoderSimple
import de.ii.xtraplatform.features.gml.domain.FeatureTokenDecoderGmlInputProfile
import de.ii.xtraplatform.features.gml.domain.ImmutableFeatureTokenDecoderGmlInputProfile
import de.ii.xtraplatform.features.gml.domain.ImmutableVariableObjectName
import de.ii.xtraplatform.geometries.domain.Geometry
import de.ii.xtraplatform.geometries.domain.GeometryType
import de.ii.xtraplatform.streams.app.ReactiveRx
import de.ii.xtraplatform.streams.domain.Reactive
import org.slf4j.LoggerFactory
import spock.lang.Shared
import spock.lang.Specification

import javax.xml.namespace.QName

/**
 * Exercises {@link FeatureTokenDecoderGml} against an ALKIS-grounded fixture mirroring
 * {@code ax_flurstueck}: short property names with SQL-column source paths and German aliases
 * that the encoder writes as XML element local names.
 */
class FeatureTokenDecoderGmlSpec extends Specification {

    @Shared Reactive reactive
    @Shared Reactive.Runner runner

    static final String ADV_NS = "http://www.adv-online.de/namespaces/adv/gid/7.1"
    static final String SF_NS = "http://www.opengis.net/ogcapi-features-1/1.0/sf"

    static final Map<String, String> NAMESPACES = [
            "adv": ADV_NS,
            "gml": "http://www.opengis.net/gml/3.2",
            "xlink": "http://www.w3.org/1999/xlink",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "sf": SF_NS
    ]

    static final List<QName> FEATURE_TYPES = [new QName(ADV_NS, "AX_Flurstueck")]
    static final EpsgCrs STORAGE_CRS = EpsgCrs.of(25832)

    def setupSpec() {
        reactive = new ReactiveRx()
        runner = reactive.runner("test-sql")
    }

    def cleanupSpec() {
        runner.close()
    }

    /**
     * AX_Flurstueck slice: {@code oid} carries role ID with SQL column {@code objid} and alias
     * {@code id}; {@code obk} is a POINT geometry with SQL column {@code obk} and alias {@code
     * objektkoordinaten}; {@code fsk} is the Flurstückskennzeichen, a plain STRING column with
     * alias {@code flurstueckskennzeichen}, used to exercise scalar value emission and {@code
     * xsi:nil} handling.
     */
    static FeatureSchema axFlurstueckSchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .label("id")
                        .alias("id"))
                .putProperties2("obk", new ImmutableFeatureSchema.Builder()
                        .sourcePath("obk")
                        .type(SchemaBase.Type.GEOMETRY)
                        .geometryType(GeometryType.POINT)
                        .label("objektkoordinaten")
                        .alias("objektkoordinaten"))
                .putProperties2("fsk", new ImmutableFeatureSchema.Builder()
                        .sourcePath("fsk")
                        .type(SchemaBase.Type.STRING)
                        .label("flurstueckskennzeichen")
                        .alias("flurstueckskennzeichen"))
                .build()
    }

    /**
     * Mirrors the encoder side: {@code useAlias} on, so the decoder matches incoming XML element
     * local names against each property's {@code alias}.
     */
    static FeatureTokenDecoderGmlInputProfile useAliasProfile() {
        ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .build()
    }

    static FeatureTokenDecoderSimple<byte[], FeatureSchema, SchemaMapping, FeatureEventHandlerSimple.ModifiableContext<FeatureSchema, SchemaMapping>> newDecoder(
            FeatureSchema schema, FeatureTokenDecoderGmlInputProfile profile,
            Optional<String> nullValue = Optional.empty(),
            Optional<EpsgCrs> headerCrs = Optional.empty()) {
        new FeatureTokenDecoderGml(
                NAMESPACES,
                FEATURE_TYPES,
                schema,
                ImmutableFeatureQuery.builder().type(schema.getName()).build(),
                Map.of(schema.getName(),
                        new ImmutableSchemaMapping.Builder()
                                .targetSchema(schema)
                                .sourcePathTransformer((path, isValue) -> path)
                                .build()),
                STORAGE_CRS,
                headerCrs,
                nullValue,
                profile)
    }

    /**
     * AX_Flurstueck slice exercising xlink/codelist routing: the codelist-valued {@code anl}
     * (anlass), the relation properties {@code "11001-21008"} (istGebucht, FEATURE_REF) and
     * {@code "11001-12001"} (zeigtAuf, FEATURE_REF_ARRAY), and the plain STRING {@code qid}
     * (quellobjektID) inherited from AA_Objekt. Property names, source paths, types and aliases
     * follow the AdV NAS schema so the test exercises real shapes.
     */
    static FeatureSchema axFlurstueckWithRefsSchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("anl", new ImmutableFeatureSchema.Builder()
                        .sourcePath("[id=rid]o11001__anl/anl_href")
                        .type(SchemaBase.Type.VALUE_ARRAY)
                        .valueType(SchemaBase.Type.STRING)
                        .alias("anlass")
                        .constraints(new ImmutableSchemaConstraints.Builder()
                                .codelist("AA_Anlassart")
                                .build()))
                .putProperties2("11001-21008", new ImmutableFeatureSchema.Builder()
                        .sourcePath("p1100121008")
                        .type(SchemaBase.Type.FEATURE_REF)
                        .valueType(SchemaBase.Type.STRING)
                        .alias("istGebucht")
                        .refType("ax_buchungsstelle"))
                .putProperties2("11001-12001", new ImmutableFeatureSchema.Builder()
                        .sourcePath("[id=rid]o11001__p1100112001/[p1100112001=objid]o12001/objid")
                        .type(SchemaBase.Type.FEATURE_REF_ARRAY)
                        .valueType(SchemaBase.Type.STRING)
                        .alias("zeigtAuf")
                        .refType("ax_lagebezeichnungohnehausnummer"))
                .putProperties2("qid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("qid")
                        .type(SchemaBase.Type.STRING)
                        .alias("quellobjektID"))
                .build()
    }

    static final FeatureTokenDecoderGmlInputProfile NAS_TEMPLATES =
            ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                    .useAlias(true)
                    .featureRefTemplate("urn:adv:oid:{{value}}")
                    .codelistUriTemplate("https://registry.gdi-de.org/codelist/de.adv-online.gid/{{codelistId}}/{{value}}")
                    .build()

    /**
     * AX_Flurstueck slice for the uom validation checks: a FLOAT property {@code afl}
     * (amtlicheFlaeche) with a {@code unit("m2")} declaration so the decoder's uom validation
     * logic engages.
     */
    static FeatureSchema axFlurstueckWithFlaecheSchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("afl", new ImmutableFeatureSchema.Builder()
                        .sourcePath("afl")
                        .type(SchemaBase.Type.FLOAT)
                        .alias("amtlicheFlaeche")
                        .unit("m2"))
                .build()
    }

    /**
     * ax_flurstueck variant with two geometry columns (POINT geometries), used to drive the
     * mixed-CRS guard and srsName resolution checks across two geometries in one feature.
     */
    static FeatureSchema twoGeometrySchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("a", new ImmutableFeatureSchema.Builder()
                        .sourcePath("a")
                        .type(SchemaBase.Type.GEOMETRY)
                        .geometryType(GeometryType.POINT)
                        .alias("a"))
                .putProperties2("b", new ImmutableFeatureSchema.Builder()
                        .sourcePath("b")
                        .type(SchemaBase.Type.GEOMETRY)
                        .geometryType(GeometryType.POINT)
                        .alias("b"))
                .build()
    }

    List<Object> runDecoder(FeatureTokenDecoderSimple decoder, String xml) {
        def stream = Reactive.Source.inputStream(new ByteArrayInputStream(xml.getBytes("UTF-8")))
                .via(decoder)
                .to(Reactive.Sink.reduce([], (list, element) -> { list << element; return list }))
        return stream.on(runner).run().toCompletableFuture().join() as List<Object>
    }

    def 'gml:id and a POINT geometry are routed to the ID and geometry properties via alias → source path'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile())
        // Encoder shape: gml:id on the feature element, <adv:objektkoordinaten> as the alias-named
        // wrapper around the gml:Point. Expected: gml:id at [o11001, objid], geometry at
        // [o11001, obk].
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:objektkoordinaten>
                <gml:Point><gml:pos>1 2</gml:pos></gml:Point>
              </adv:objektkoordinaten>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.first() == FeatureTokenType.INPUT
        tokens.last() == FeatureTokenType.INPUT_END
        tokens.count { it == FeatureTokenType.FEATURE } == 1
        tokens.count { it == FeatureTokenType.FEATURE_END } == 1

        valueAtPath(tokens, ["oid"]) == "DENW36AL10000XYZ"

        def geometries = tokens.findAll { it instanceof Geometry } as List<Geometry>
        geometries.size() == 1
        geometries[0].type == GeometryType.POINT
        pathBeforeGeometry(tokens) == ["obk"]
    }

    def 'a scalar property text value is emitted at the property source path'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:flurstueckskennzeichen>05334001001000010001__</adv:flurstueckskennzeichen>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        // The fsk value is emitted at the SQL column source path, not at the alias.
        valueAtPath(tokens, ["fsk"]) == "05334001001000010001__"
    }

    def 'xsi:nil="true" emits the configured null marker'() {
        given:
        // nilReason on the same element must not leak as a token.
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile(), Optional.of("__NULL__"))
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                gml:id="DENW36AL10000XYZ">
              <adv:flurstueckskennzeichen xsi:nil="true" nilReason="missing"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["fsk"]) == "__NULL__"
        !tokens.contains("missing")
    }

    def 'xsi:nil with no configured nullValue drops the property value'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                gml:id="DENW36AL10000XYZ">
              <adv:flurstueckskennzeichen xsi:nil="true"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        // gml:id is the only value emitted; the nil scalar property is dropped.
        tokens.count { it == FeatureTokenType.VALUE } == 1
        valueAtPath(tokens, ["oid"]) == "DENW36AL10000XYZ"
    }

    def 'geometry srsName takes precedence over header and storage CRS'() {
        given:
        // header is set, storage is the default 25832; srsName on the gml:Point must win.
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile(),
                Optional.empty(), Optional.of(EpsgCrs.of(4326)))
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:objektkoordinaten>
                <gml:Point srsName="urn:ogc:def:crs:EPSG::28992"><gml:pos>1 2</gml:pos></gml:Point>
              </adv:objektkoordinaten>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        def geometry = tokens.find { it instanceof Geometry } as Geometry
        geometry.crs.get() == EpsgCrs.of(28992)
    }

    def 'geometry without srsName falls back to header CRS'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile(),
                Optional.empty(), Optional.of(EpsgCrs.of(4326)))
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:objektkoordinaten>
                <gml:Point><gml:pos>1 2</gml:pos></gml:Point>
              </adv:objektkoordinaten>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        def geometry = tokens.find { it instanceof Geometry } as Geometry
        geometry.crs.get() == EpsgCrs.of(4326)
    }

    def 'geometry without srsName or header falls back to storage CRS'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:objektkoordinaten>
                <gml:Point><gml:pos>1 2</gml:pos></gml:Point>
              </adv:objektkoordinaten>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        def geometry = tokens.find { it instanceof Geometry } as Geometry
        geometry.crs.get() == STORAGE_CRS
    }

    def 'mixed CRSs across geometries in one feature are rejected'() {
        given:
        def decoder = newDecoder(twoGeometrySchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:a><gml:Point srsName="urn:ogc:def:crs:EPSG::28992"><gml:pos>1 2</gml:pos></gml:Point></adv:a>
              <adv:b><gml:Point srsName="urn:ogc:def:crs:EPSG::25832"><gml:pos>3 4</gml:pos></gml:Point></adv:b>
            </adv:AX_Flurstueck>"""

        when:
        runDecoder(decoder, xml)

        then:
        def e = thrown(Exception)
        rootCauseMessage(e).contains("same CRS")
    }

    def 'srsNameMappings resolves an ADV URN to the configured EpsgCrs'() {
        given:
        // Built-in srsName parser does not understand ADV URN forms; without the mapping
        // the geometry would fall back to storage / header CRS. DE_DHDN_3GK2_NW101 is a
        // 3-degree Gauss-Krueger zone 2 realization → EPSG:31466.
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .putSrsNameMappings("urn:adv:crs:DE_DHDN_3GK2_NW101", EpsgCrs.of(31466))
                .build()
        def decoder = newDecoder(axFlurstueckSchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:objektkoordinaten>
                <gml:Point srsName="urn:adv:crs:DE_DHDN_3GK2_NW101"><gml:pos>1 2</gml:pos></gml:Point>
              </adv:objektkoordinaten>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        def geometry = tokens.find { it instanceof Geometry } as Geometry
        geometry.crs.get() == EpsgCrs.of(31466)
    }

    def 'srsNameMappings interacts correctly with the mixed-CRS guard'() {
        given:
        // Two ADV URN realizations both resolved to EPSG:31466. The mixed-CRS check
        // compares resolved EpsgCrs values, not raw URNs, so these must NOT trip the guard.
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .putSrsNameMappings("urn:adv:crs:DE_DHDN_3GK2_NW101", EpsgCrs.of(31466))
                .putSrsNameMappings("urn:adv:crs:DE_DHDN_3GK2_NW177", EpsgCrs.of(31466))
                .build()
        def decoder = newDecoder(twoGeometrySchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:a><gml:Point srsName="urn:adv:crs:DE_DHDN_3GK2_NW101"><gml:pos>1 2</gml:pos></gml:Point></adv:a>
              <adv:b><gml:Point srsName="urn:adv:crs:DE_DHDN_3GK2_NW177"><gml:pos>3 4</gml:pos></gml:Point></adv:b>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        noExceptionThrown()
        def geometries = tokens.findAll { it instanceof Geometry } as List<Geometry>
        geometries.size() == 2
        geometries.every { it.crs.get() == EpsgCrs.of(31466) }
    }

    def 'codelist xlink:href on adv:anlass is reduced to the bare code via codelistUriTemplate'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        // anl is the AA_Anlassart codelist on AX_Flurstueck. The encoder emits xlink:href +
        // xlink:title; the decoder reverses codelistUriTemplate so that the emitted value is
        // the raw codelist value ("010704") and drops xlink:title.
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:anlass xlink:href="https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_Anlassart/010704"
                          xlink:title="Qualitätssicherung und Datenpflege"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.contains("010704")
        !tokens.contains("https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_Anlassart/010704")
        !tokens.contains("Qualitätssicherung und Datenpflege")
    }

    def 'feature-ref xlink:href on adv:istGebucht is reduced to the bare feature id via featureRefTemplate'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:istGebucht xlink:href="urn:adv:oid:DENW36ALl800005x"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["11001-21008"]) == "DENW36ALl800005x"
        !tokens.contains("urn:adv:oid:DENW36ALl800005x")
    }

    def 'feature-ref-array xlink:href on adv:zeigtAuf is reduced for each member via featureRefTemplate'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:zeigtAuf xlink:href="urn:adv:oid:DENW36AL00000AAA"/>
              <adv:zeigtAuf xlink:href="urn:adv:oid:DENW36AL00000BBB"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.contains("DENW36AL00000AAA")
        tokens.contains("DENW36AL00000BBB")
        !tokens.contains("urn:adv:oid:DENW36AL00000AAA")
    }

    static final FeatureTokenDecoderGmlInputProfile NAS_TEMPLATES_SUFFIXED =
            ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                    .useAlias(true)
                    .featureRefTemplate("urn:adv:oid:{{value}}")
                    // Declared by property id "11001-21008", not the on-the-wire alias istGebucht.
                    .addObjectTypeSuffixedProperties("11001-21008")
                    .build()

    def 'objectTypeSuffixedProperties: a _<ObjectType>-suffixed element maps to the base feature-ref property'() {
        given:
        // ALKIS NAS names this element adv:gehoertZuBauwerk_AX_Turm; property "11001-21008" (alias
        // istGebucht) stands in here as a declared suffixed property. The _AX_Turm suffix is ignored
        // and the href is reduced as for the plain element.
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES_SUFFIXED)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:istGebucht_AX_Turm xlink:href="urn:adv:oid:DENW36ALl800005x"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["11001-21008"]) == "DENW36ALl800005x"
    }

    def 'objectTypeSuffixedProperties: the unsuffixed element still matches a declared property'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES_SUFFIXED)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:istGebucht xlink:href="urn:adv:oid:DENW36ALl800005x"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["11001-21008"]) == "DENW36ALl800005x"
    }

    def 'a suffixed element is not matched when the property is not declared in objectTypeSuffixedProperties'() {
        given:
        // Same element, but the default profile declares no suffixed properties: the suffix is not
        // stripped, the element matches no property and is skipped (the FK is not emitted).
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:istGebucht_AX_Turm xlink:href="urn:adv:oid:DENW36ALl800005x"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["11001-21008"]) == null
    }

    def 'xlink:href is emitted unchanged when no template is configured'() {
        given:
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .build()
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:istGebucht xlink:href="urn:adv:oid:DENW36ALl800005x"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["11001-21008"]) == "urn:adv:oid:DENW36ALl800005x"
    }

    def 'xlink:href is emitted unchanged when the configured template does not match, and a WARN is logged'() {
        // The fall-through to the raw href is necessary (we have no better value to emit) but
        // dangerous in practice — the URI is almost always wider than the storage column and longer
        // than any expected value. A no-match is therefore a configuration mismatch the operator
        // needs to see, not a silent best-effort.
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:istGebucht xlink:href="https://example.org/items/some-other-id"/>
            </adv:AX_Flurstueck>"""

        when:
        List<Object> tokens = null
        List<ILoggingEvent> warnings = captureWarnings { tokens = runDecoder(decoder, xml) }

        then:
        valueAtPath(tokens, ["11001-21008"]) == "https://example.org/items/some-other-id"
        warnings.size() == 1
        def msg = warnings[0].formattedMessage
        msg.contains("https://example.org/items/some-other-id")
        msg.contains("does not match the configured template")
    }

    def 'codelist xlink:href that does not match the configured template logs a WARN'() {
        // Real-world manifestation: a service config whose codelistUriTemplate uses a namespace
        // segment (e.g. 'de.adv.alkis') that differs from the namespace segment in the actual
        // NAS data ('de.adv-online.gid'). Today the raw URI flowed silently into the SQL layer
        // and surfaced as a varchar overflow at insert time; a WARN gives the operator a clear
        // 'fix your template' signal at decode time instead.
        given:
        def mismatchedProfile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .codelistUriTemplate("https://registry.gdi-de.org/codelist/de.adv.alkis/{{codelistId}}/{{value}}")
                .build()
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), mismatchedProfile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:anlass xlink:href="https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_Anlassart/010704"
                          xlink:title="Qualitätssicherung und Datenpflege"/>
            </adv:AX_Flurstueck>"""

        when:
        List<Object> tokens = null
        List<ILoggingEvent> warnings = captureWarnings { tokens = runDecoder(decoder, xml) }

        then: 'the unchanged href is emitted as the value (will fail downstream)'
        tokens.contains("https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_Anlassart/010704")

        and: 'a WARN identifies the mismatching href and the configured template with the codelistId substituted'
        warnings.size() == 1
        def msg = warnings[0].formattedMessage
        msg.contains("https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_Anlassart/010704")
        msg.contains("https://registry.gdi-de.org/codelist/de.adv.alkis/AA_Anlassart/{{value}}")
    }

    def 'matching xlink:href does not log a WARN'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:istGebucht xlink:href="urn:adv:oid:DENW36ALl800005x"/>
              <adv:anlass xlink:href="https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_Anlassart/010704"/>
            </adv:AX_Flurstueck>"""

        when:
        List<Object> tokens = null
        List<ILoggingEvent> warnings = captureWarnings { tokens = runDecoder(decoder, xml) }

        then:
        warnings.empty
        tokens.contains("DENW36ALl800005x")
        tokens.contains("010704")
    }

    def 'wrap=OBJECT FEATURE_REF: xlink:href on the property element emits the reduced id at the .id child'() {
        given:
        // Mirrors the live schema shape after an upstream wrap=OBJECT transformation expands a
        // FEATURE_REF into an OBJECT with id/title/type children — the form
        // FeatureEncoderSql actually receives in production. The id child carries the
        // sourcePath of the underlying column; the type child is a constant marker (ignored
        // here). The decoder must read xlink:href from the property element itself and emit it
        // as the .id child's value so the writable column wired to id receives the bare ref id.
        def schema = new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("11001-21008", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.OBJECT)
                        .alias("istGebucht")
                        .refType("ax_buchungsstelle")
                        .putProperties2("id", new ImmutableFeatureSchema.Builder()
                                .sourcePath("p1100121008")
                                .type(SchemaBase.Type.STRING))
                        .putProperties2("title", new ImmutableFeatureSchema.Builder()
                                .sourcePath("p1100121008")
                                .type(SchemaBase.Type.STRING))
                        .putProperties2("type", new ImmutableFeatureSchema.Builder()
                                .sourcePath("constant_11001_21008_x")
                                .type(SchemaBase.Type.STRING)
                                .constantValue("ax_buchungsstelle")))
                .build()
        def decoder = newDecoder(schema, NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:istGebucht xlink:href="urn:adv:oid:DENW36ALl800005x"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        // OBJECT bookends sit at the property's own path …
        indexOfTokenAtPath(tokens, FeatureTokenType.OBJECT, ["11001-21008"]) >= 0
        indexOfTokenAtPath(tokens, FeatureTokenType.OBJECT_END, ["11001-21008"]) >= 0
        // … and the reduced ref id is emitted at the conventional .id child path.
        valueAtPath(tokens, ["11001-21008", "id"]) == "DENW36ALl800005x"
        !tokens.contains("urn:adv:oid:DENW36ALl800005x")
    }

    def 'wrap=OBJECT_ARRAY FEATURE_REF_ARRAY: each xlink:href member emits its reduced id at the .id child, wrapped in ARRAY/ARRAY_END'() {
        given:
        // Mirrors the live schema shape after wrap=OBJECT_ARRAY expands a FEATURE_REF_ARRAY:
        // the property is OBJECT_ARRAY with id/title/type children, refType set. Each wire
        // sibling xlink:href becomes one OBJECT_START/.id-onValue/OBJECT_END triple inside a
        // single ARRAY/ARRAY_END pair at the feature root. The encoder's onObjectStart picks
        // up getTableForObject and opens one junction row per member; the .id onValue fills
        // the FK column on that row.
        def schema = new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("11001-12001", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.OBJECT_ARRAY)
                        .alias("zeigtAuf")
                        .refType("ax_lagebezeichnungohnehausnummer")
                        .putProperties2("id", new ImmutableFeatureSchema.Builder()
                                .sourcePath("[id=rid]o11001__p1100112001/p1100112001")
                                .type(SchemaBase.Type.STRING))
                        .putProperties2("title", new ImmutableFeatureSchema.Builder()
                                .sourcePath("[id=rid]o11001__p1100112001/p1100112001")
                                .type(SchemaBase.Type.STRING))
                        .putProperties2("type", new ImmutableFeatureSchema.Builder()
                                .sourcePath("constant_11001_12001_x")
                                .type(SchemaBase.Type.STRING)
                                .constantValue("ax_lagebezeichnungohnehausnummer")))
                .build()
        def decoder = newDecoder(schema, NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:zeigtAuf xlink:href="urn:adv:oid:DENW36AL00000AAA"/>
              <adv:zeigtAuf xlink:href="urn:adv:oid:DENW36AL00000BBB"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        // Exactly one ARRAY pair brackets all members at the OBJECT_ARRAY's path.
        def arrayStart = indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY, ["11001-12001"])
        def arrayEnd = indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY_END, ["11001-12001"])
        arrayStart >= 0 && arrayEnd > arrayStart
        // Two OBJECT bookend pairs sit at the OBJECT_ARRAY's path between the brackets.
        def objectStarts = indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT, ["11001-12001"])
        objectStarts.size() == 2
        objectStarts.every { it > arrayStart && it < arrayEnd }
        // Each member's reduced ref id is emitted at the conventional .id child path.
        tokens.contains("DENW36AL00000AAA")
        tokens.contains("DENW36AL00000BBB")
        !tokens.contains("urn:adv:oid:DENW36AL00000AAA")
    }

    def 'xlink:href on a STRING property with empty body is emitted as the value (fallback)'() {
        given:
        // qid (quellobjektID) is a plain STRING inherited from aa_objekt — no codelist, not a
        // feature-ref. When the element has no text content but carries an xlink:href, the
        // decoder routes the href through the featureRefTemplate reverse substitution as a
        // fallback so that the FEATURE_REF_ARRAY → VALUE_ARRAY workaround keeps working on the
        // wire. The featureRefTemplate matches `urn:adv:oid:{{value}}` here, so an unrelated href
        // like `urn:something:else:42` falls through unchanged.
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:quellobjektID xlink:href="urn:adv:oid:DENW36AL10000QID"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.contains("DENW36AL10000QID")
        !tokens.contains("urn:adv:oid:DENW36AL10000QID")
    }

    def 'xlink:href on a STRING property whose href does not match featureRefTemplate is emitted unchanged'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:quellobjektID xlink:href="urn:something:else:42"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.contains("urn:something:else:42")
    }

    def 'xlink:href takes precedence over text content when both are present on a codelist property'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:anlass xlink:href="https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_Anlassart/010704">human label</adv:anlass>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.contains("010704")
        !tokens.contains("human label")
    }

    def 'uom attribute is dropped from the token stream and silently passes when it matches the schema unit'() {
        given:
        // Encoder direction: schema unit "m2" → wire "urn:adv:uom:m2" (via UomMapping +
        // UomStyle.TEMPLATE). Reverse on input: wire URN as key, canonical "m2" as value. The
        // uom attribute itself is not surfaced — the canonical unit is already on the schema.
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .putUomMappings("urn:adv:uom:m2", "m2")
                .build()
        def decoder = newDecoder(axFlurstueckWithFlaecheSchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:amtlicheFlaeche uom="urn:adv:uom:m2">1234.56</adv:amtlicheFlaeche>
            </adv:AX_Flurstueck>"""

        List<Object> tokens = null
        List<ILoggingEvent> warnings = captureWarnings { tokens = runDecoder(decoder, xml) }

        expect:
        // The numeric value is emitted; the uom attribute (in either wire URN or canonical
        // form) is not.
        tokens.contains("1234.56")
        !tokens.contains("urn:adv:uom:m2")
        !tokens.contains("m2")
        warnings.empty
    }

    def 'mismatched uom (after reverse-mapping) logs a warning'() {
        given:
        // Wire value is unmapped and does not match the schema unit "m2".
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .putUomMappings("urn:adv:uom:m2", "m2")
                .build()
        def decoder = newDecoder(axFlurstueckWithFlaecheSchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:amtlicheFlaeche uom="urn:adv:uom:km2">7.0</adv:amtlicheFlaeche>
            </adv:AX_Flurstueck>"""

        List<Object> tokens = null
        List<ILoggingEvent> warnings = captureWarnings { tokens = runDecoder(decoder, xml) }

        expect:
        // The attribute is still dropped from the stream (validation only).
        !tokens.contains("urn:adv:uom:km2")
        // A warning is emitted naming the wire uom and the schema unit.
        warnings.any {
            it.formattedMessage.contains("urn:adv:uom:km2") &&
                    it.formattedMessage.contains("m2")
        }
    }

    def 'uom attribute on a property without a declared unit is silently dropped'() {
        given:
        // qid is a plain STRING with no schema-declared unit, so a stray uom attribute is just
        // dropped without a warning.
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:quellobjektID uom="urn:adv:uom:m2">stray</adv:quellobjektID>
            </adv:AX_Flurstueck>"""

        List<Object> tokens = null
        List<ILoggingEvent> warnings = captureWarnings { tokens = runDecoder(decoder, xml) }

        expect:
        !tokens.contains("urn:adv:uom:m2")
        warnings.empty
    }

    private static List<ILoggingEvent> captureWarnings(Closure block) {
        def appender = new ListAppender<ILoggingEvent>()
        appender.start()
        def logger = (LogbackLogger) LoggerFactory.getLogger(FeatureTokenDecoderGml)
        logger.addAppender(appender)
        try {
            block.call()
        } finally {
            logger.detachAppender(appender)
        }
        return appender.list.findAll { it.level == Level.WARN }
    }

    def 'gmlIdPrefix strips the configured prefix from the emitted feature id'() {
        given:
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .gmlIdPrefix("DENW36AL")
                .build()
        def decoder = newDecoder(axFlurstueckSchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}" xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ"/>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["oid"]) == "10000XYZ"
        !tokens.contains("DENW36AL10000XYZ")
    }

    def 'gmlIdPrefix leaves a non-matching gml:id unchanged'() {
        given:
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .gmlIdPrefix("OTHER_")
                .build()
        def decoder = newDecoder(axFlurstueckSchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}" xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ"/>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["oid"]) == "DENW36AL10000XYZ"
    }

    def 'a single-member VALUE_ARRAY emits ARRAY around the one value'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        // anl is VALUE_ARRAY (codelist). Even a single occurrence must be bracketed so the
        // downstream consumer sees an array, not a scalar.
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:anlass xlink:href="https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_Anlassart/010704"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.count { it == FeatureTokenType.ARRAY } == 1
        tokens.count { it == FeatureTokenType.ARRAY_END } == 1
        // ARRAY token is followed by the path the value sits at.
        def arrayIdx = tokens.findIndexOf { it == FeatureTokenType.ARRAY }
        tokens[arrayIdx + 1] == ["anl"]
        // The reduced codelist value sits inside the array brackets.
        def arrayEndIdx = tokens.findIndexOf { it == FeatureTokenType.ARRAY_END }
        def codeValueIdx = tokens.indexOf("010704")
        arrayIdx < codeValueIdx
        codeValueIdx < arrayEndIdx
    }

    def 'a multi-member FEATURE_REF_ARRAY brackets all members in one ARRAY pair'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        // zeigtAuf carries two members on the wire; both must sit inside a single ARRAY/ARRAY_END
        // pair at the property's source path.
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:zeigtAuf xlink:href="urn:adv:oid:DENW36AL00000AAA"/>
              <adv:zeigtAuf xlink:href="urn:adv:oid:DENW36AL00000BBB"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.count { it == FeatureTokenType.ARRAY } == 1
        tokens.count { it == FeatureTokenType.ARRAY_END } == 1
        // Both members emitted inside the array.
        def arrayIdx = tokens.findIndexOf { it == FeatureTokenType.ARRAY }
        def arrayEndIdx = tokens.findIndexOf { it == FeatureTokenType.ARRAY_END }
        def memberIndices = tokens.findIndexValues { it == "DENW36AL00000AAA" || it == "DENW36AL00000BBB" }
        memberIndices.size() == 2
        memberIndices.every { (long) arrayIdx < it && it < (long) arrayEndIdx }
    }

    def 'the open array closes when the wire moves on to a non-array sibling property'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        // anl (VALUE_ARRAY) followed by quellobjektID (plain STRING). The array must close before
        // the scalar property emits.
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:anlass xlink:href="https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_Anlassart/010704"/>
              <adv:quellobjektID>some-source-id</adv:quellobjektID>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        def arrayEndIdx = tokens.findIndexOf { it == FeatureTokenType.ARRAY_END }
        def qidValueIdx = tokens.findIndexOf { i ->
            tokens.indexOf(i) > arrayEndIdx && i == "some-source-id"
        }
        arrayEndIdx > 0
        // The plain STRING value follows after the array closes.
        tokens.indexOf("some-source-id") > arrayEndIdx
    }

    def 'a single FEATURE_REF (non-array) emits no ARRAY tokens'() {
        given:
        def decoder = newDecoder(axFlurstueckWithRefsSchema(), NAS_TEMPLATES)
        // istGebucht is FEATURE_REF (single, not array) — never brackets.
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL10000XYZ">
              <adv:istGebucht xlink:href="urn:adv:oid:DENW36ALl800005x"/>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        !tokens.contains(FeatureTokenType.ARRAY)
        !tokens.contains(FeatureTokenType.ARRAY_END)
    }

    def 'xmlAttributes routes an unqualified attribute on the feature root to the child property source path'() {
        given:
        // Encoder direction: a STRING property listed in xmlAttributes is written as an unqualified
        // attribute on the parent object element (the feature element here). On the wire, the
        // attribute name follows the same alias/name rule as element naming. Here fsk
        // (flurstueckskennzeichen) is listed; the wire form omits the <adv:flurstueckskennzeichen>
        // element and instead carries flurstueckskennzeichen="…" on the feature.
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .addXmlAttributes("fsk")
                .build()
        def decoder = newDecoder(axFlurstueckSchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ"
                flurstueckskennzeichen="05334001001000010001__"/>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["fsk"]) == "05334001001000010001__"
    }

    def 'an unqualified attribute on the feature root is ignored when xmlAttributes is empty'() {
        given:
        // Without xmlAttributes configured, an unrelated attribute on the feature element must
        // not surface as a token — only the gml:id value is emitted.
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ"
                flurstueckskennzeichen="05334001001000010001__"/>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.count { it == FeatureTokenType.VALUE } == 1
        valueAtPath(tokens, ["oid"]) == "DENW36AL10000XYZ"
        !tokens.contains("05334001001000010001__")
    }

    /**
     * Mirrors the AX_Flurstueck → gmk(AX_Gemarkung_Schluessel) → {land, gemarkungsnummer} chain
     * from the AdV NAS schema. The nested OBJECT property carries an explicit sourcePath
     * segment so the emitted child paths are unambiguous in this test; flattening (OBJECT
     * without sourcePath, where child source paths live in the parent table) is left for a
     * follow-up.
     */
    static FeatureSchema axFlurstueckWithGemarkungSchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("gmk", new ImmutableFeatureSchema.Builder()
                        .sourcePath("gmk")
                        .type(SchemaBase.Type.OBJECT)
                        .objectType("AX_Gemarkung_Schluessel")
                        .alias("gemarkung")
                        .putProperties2("lan", new ImmutableFeatureSchema.Builder()
                                .sourcePath("lan")
                                .type(SchemaBase.Type.STRING)
                                .alias("land"))
                        .putProperties2("gmn", new ImmutableFeatureSchema.Builder()
                                .sourcePath("gmn")
                                .type(SchemaBase.Type.STRING)
                                .alias("gemarkungsnummer")))
                .putProperties2("fsk", new ImmutableFeatureSchema.Builder()
                        .sourcePath("fsk")
                        .type(SchemaBase.Type.STRING)
                        .alias("flurstueckskennzeichen"))
                .build()
    }

    def 'nested OBJECT children are resolved against the OBJECT schema and emit at the nested source path'() {
        given:
        // NAS wire form: <adv:gemarkung> (alias for OBJECT prop gmk) wraps the object-type element
        // <adv:AX_Gemarkung_Schluessel>, which in turn carries the scalar children <adv:land> and
        // <adv:gemarkungsnummer>. The wrapper element contributes no path segment.
        def decoder = newDecoder(axFlurstueckWithGemarkungSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:gemarkung>
                <adv:AX_Gemarkung_Schluessel>
                  <adv:land>05</adv:land>
                  <adv:gemarkungsnummer>4320</adv:gemarkungsnummer>
                </adv:AX_Gemarkung_Schluessel>
              </adv:gemarkung>
              <adv:flurstueckskennzeichen>05432002500008______</adv:flurstueckskennzeichen>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["gmk", "lan"]) == "05"
        valueAtPath(tokens, ["gmk", "gmn"]) == "4320"
        // The depth-1 sibling after the nested OBJECT must land at its own depth-1 path,
        // proving that the path tracker shortens when descending back out of the nested object.
        valueAtPath(tokens, ["fsk"]) == "05432002500008______"
    }

    def 'an OBJECT property with two object-element children is rejected as unsupported'() {
        given:
        // GML's usual property pattern is one property element wrapping a single nested object
        // element. The spec does also allow GML *array properties*, where one property element
        // wraps multiple peer object elements, but those are rare in feature data and do not
        // appear in the NAS schema this decoder targets — so we reject the shape here. If array
        // properties ever need to be supported, the OBJECT_PROPERTY branch in
        // FeatureTokenDecoderGml.onStartElement (which currently throws on a second
        // object-element child) needs to be reworked to emit one OBJECT_START/OBJECT_END per
        // child element under one ARRAY/ARRAY_END pair.
        def decoder = newDecoder(axFlurstueckWithGemarkungSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:gemarkung>
                <adv:AX_Gemarkung_Schluessel>
                  <adv:land>05</adv:land>
                  <adv:gemarkungsnummer>4320</adv:gemarkungsnummer>
                </adv:AX_Gemarkung_Schluessel>
                <adv:AX_Gemarkung_Schluessel>
                  <adv:land>06</adv:land>
                  <adv:gemarkungsnummer>4321</adv:gemarkungsnummer>
                </adv:AX_Gemarkung_Schluessel>
              </adv:gemarkung>
            </adv:AX_Flurstueck>"""

        when:
        runDecoder(decoder, xml)

        then:
        def e = thrown(Exception)
        def msg = rootCauseMessage(e)
        msg.contains("object property")
        msg.contains("gmk")
    }

    def 'unknown children inside a nested OBJECT are ignored without disturbing sibling values'() {
        given:
        def decoder = newDecoder(axFlurstueckWithGemarkungSchema(), useAliasProfile())
        // <adv:foobar> has no matching property under AX_Gemarkung_Schluessel; the decoder must
        // descend through it (without emitting) and still resolve <adv:gemarkungsnummer> correctly.
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:gemarkung>
                <adv:AX_Gemarkung_Schluessel>
                  <adv:land>05</adv:land>
                  <adv:foobar>ignored</adv:foobar>
                  <adv:gemarkungsnummer>4320</adv:gemarkungsnummer>
                </adv:AX_Gemarkung_Schluessel>
              </adv:gemarkung>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["gmk", "lan"]) == "05"
        valueAtPath(tokens, ["gmk", "gmn"]) == "4320"
        !tokens.contains("ignored")
    }

    /**
     * AX_Wohnbauflaeche slice exercising two shapes that AdV NAS uses heavily: an OBJECT_ARRAY
     * of {@code modellart} (each member wraps the {@code AA_Modellart} type) and the two-level
     * nested {@code zeigtAufExternes} → {@code fachdatenobjekt} chain. Property names, source
     * paths and aliases follow AA_Objekt, AA_Modellart, AA_Fachdatenverbindung and
     * AA_Fachdatenobjekt.
     */
    static FeatureSchema axWohnbauflaecheSchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_wohnbauflaeche")
                .sourcePath("/o41001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("mat", new ImmutableFeatureSchema.Builder()
                        .sourcePath("[id=rid]o02330__mat")
                        .type(SchemaBase.Type.OBJECT_ARRAY)
                        .objectType("AA_Modellart")
                        .alias("modellart")
                        .putProperties2("stm", new ImmutableFeatureSchema.Builder()
                                .sourcePath("stm")
                                .type(SchemaBase.Type.STRING)
                                .alias("advStandardModell"))
                        .putProperties2("som", new ImmutableFeatureSchema.Builder()
                                .sourcePath("som")
                                .type(SchemaBase.Type.STRING)
                                .alias("sonstigesModell")
                                .constraints(new ImmutableSchemaConstraints.Builder()
                                        .codelist("AA_WeitereModellart")
                                        .build())))
                .putProperties2("fdv", new ImmutableFeatureSchema.Builder()
                        .sourcePath("[id=rid]o02330__fdv")
                        .type(SchemaBase.Type.OBJECT_ARRAY)
                        .objectType("AA_Fachdatenverbindung")
                        .alias("zeigtAufExternes")
                        .putProperties2("art", new ImmutableFeatureSchema.Builder()
                                .sourcePath("art")
                                .type(SchemaBase.Type.STRING)
                                .alias("art"))
                        .putProperties2("fdo", new ImmutableFeatureSchema.Builder()
                                .type(SchemaBase.Type.OBJECT)
                                .objectType("AA_Fachdatenobjekt")
                                .alias("fachdatenobjekt")
                                .putProperties2("uri", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("fdo__uri")
                                        .type(SchemaBase.Type.STRING)
                                        .alias("uri"))))
                .build()
    }

    static FeatureTokenDecoderSimple<byte[], FeatureSchema, SchemaMapping, FeatureEventHandlerSimple.ModifiableContext<FeatureSchema, SchemaMapping>> newWohnbauflaecheDecoder(
            FeatureTokenDecoderGmlInputProfile profile) {
        def schema = axWohnbauflaecheSchema()
        new FeatureTokenDecoderGml(
                NAMESPACES,
                [new QName(ADV_NS, "AX_Wohnbauflaeche")],
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
                profile)
    }

    def 'AX_Wohnbauflaeche modellart OBJECT_ARRAY and zeigtAufExternes two-level nested OBJECT'() {
        given:
        // Codelist template needed because the second modellart member carries sonstigesModell as
        // xlink:href to the AA_WeitereModellart codelist; the decoder reverses it to bare "NWABK".
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .codelistUriTemplate("https://registry.gdi-de.org/codelist/de.adv-online.gid/{{codelistId}}/{{value}}")
                .build()
        def decoder = newWohnbauflaecheDecoder(profile)
        // Wire form lifted from src/test/resources/nas/AX_Wohnbauflaeche.xml — two modellart array
        // members followed by a zeigtAufExternes whose AA_Fachdatenverbindung wraps a nested
        // AA_Fachdatenobjekt with its own uri child.
        def xml = """<adv:AX_Wohnbauflaeche xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL2mD000Cg">
              <adv:modellart>
                <adv:AA_Modellart>
                  <adv:advStandardModell>DLKM</adv:advStandardModell>
                </adv:AA_Modellart>
              </adv:modellart>
              <adv:modellart>
                <adv:AA_Modellart>
                  <adv:sonstigesModell xlink:href="https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_WeitereModellart/NWABK"/>
                </adv:AA_Modellart>
              </adv:modellart>
              <adv:zeigtAufExternes>
                <adv:AA_Fachdatenverbindung>
                  <adv:art>urn:adv:fdv:0901</adv:art>
                  <adv:fachdatenobjekt>
                    <adv:AA_Fachdatenobjekt>
                      <adv:uri>urn:adv:oid:DENW36PS00001I8A</adv:uri>
                    </adv:AA_Fachdatenobjekt>
                  </adv:fachdatenobjekt>
                </adv:AA_Fachdatenverbindung>
              </adv:zeigtAufExternes>
            </adv:AX_Wohnbauflaeche>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        // Paths are dotted property-name chains, matching the SqlQueryMapping's column keys
        // (e.g. {@code mat.stm}, {@code fdv.fdo.uri}). The nested OBJECT property fdo contributes
        // its own name as a path segment, so its child {@code uri} sits at fdv → fdo → uri.
        def matPath = ["mat"]
        def fdvPath = ["fdv"]
        valueAtPath(tokens, matPath + "stm") == "DLKM"
        valueAtPath(tokens, matPath + "som") == "NWABK"
        valueAtPath(tokens, fdvPath + "art") == "urn:adv:fdv:0901"
        valueAtPath(tokens, fdvPath + ["fdo", "uri"]) == "urn:adv:oid:DENW36PS00001I8A"

        // Array brackets around the OBJECT_ARRAY members; the path tracker still points at the
        // array property when each bracket is emitted.
        def matArrayStart = indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY, matPath)
        def matArrayEnd = indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY_END, matPath)
        matArrayStart >= 0
        matArrayEnd > matArrayStart

        // Two OBJECT/OBJECT_END pairs for the two modellart members, both inside the array.
        def matObjectStarts = indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT, matPath)
        def matObjectEnds = indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT_END, matPath)
        matObjectStarts.size() == 2
        matObjectEnds.size() == 2
        matObjectStarts.every { it > matArrayStart && it < matArrayEnd }
        matObjectEnds.every { it > matArrayStart && it < matArrayEnd }

        // One OBJECT_START / OBJECT_END pair at the fdv member's level (its sole array entry),
        // plus an inner pair at fdv → fdo for the nested fachdatenobjekt OBJECT.
        def fdvObjectStarts = indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT, fdvPath)
        def fdvObjectEnds = indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT_END, fdvPath)
        fdvObjectStarts.size() == 1
        fdvObjectEnds.size() == 1
        indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT, fdvPath + "fdo").size() == 1
        indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT_END, fdvPath + "fdo").size() == 1
    }

    def 'GML array property shape: one OBJECT_PROPERTY wrapping multiple peer OBJECT_ELEMENTs emits per-peer OBJECT bookends inside one ARRAY pair'() {
        given:
        // Same OBJECT_ARRAY schema as the previous spec (mat = AA_Modellart), but the wire shape
        // collapses the two members into ONE <adv:modellart> property element wrapping two peer
        // <adv:AA_Modellart> children. The downstream token stream must be indistinguishable from
        // the multi-property-element shape.
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .codelistUriTemplate("https://registry.gdi-de.org/codelist/de.adv-online.gid/{{codelistId}}/{{value}}")
                .build()
        def decoder = newWohnbauflaecheDecoder(profile)
        def xml = """<adv:AX_Wohnbauflaeche xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL2mD000Cg">
              <adv:modellart>
                <adv:AA_Modellart>
                  <adv:advStandardModell>DLKM</adv:advStandardModell>
                </adv:AA_Modellart>
                <adv:AA_Modellart>
                  <adv:sonstigesModell xlink:href="https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_WeitereModellart/NWABK"/>
                </adv:AA_Modellart>
              </adv:modellart>
            </adv:AX_Wohnbauflaeche>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        def matPath = ["mat"]
        valueAtPath(tokens, matPath + "stm") == "DLKM"
        valueAtPath(tokens, matPath + "som") == "NWABK"

        and: 'exactly one ARRAY pair brackets both members'
        def matArrayStart = indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY, matPath)
        def matArrayEnd = indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY_END, matPath)
        matArrayStart >= 0
        matArrayEnd > matArrayStart
        indicesOfTokenAtPath(tokens, FeatureTokenType.ARRAY, matPath).size() == 1
        indicesOfTokenAtPath(tokens, FeatureTokenType.ARRAY_END, matPath).size() == 1

        and: 'two OBJECT bookend pairs sit inside the single ARRAY pair'
        def matObjectStarts = indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT, matPath)
        def matObjectEnds = indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT_END, matPath)
        matObjectStarts.size() == 2
        matObjectEnds.size() == 2
        matObjectStarts.every { it > matArrayStart && it < matArrayEnd }
        matObjectEnds.every { it > matArrayStart && it < matArrayEnd }
    }

    def 'GML array property shape: a single peer OBJECT_ELEMENT inside an array OBJECT_PROPERTY still produces one OBJECT pair'() {
        given:
        // Mixed shapes for the same OBJECT_ARRAY schema: the first <adv:modellart> uses the
        // single-peer wire shape, the second uses the multi-peer shape. Three members in total —
        // three OBJECT pairs, all inside one ARRAY pair.
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .codelistUriTemplate("https://registry.gdi-de.org/codelist/de.adv-online.gid/{{codelistId}}/{{value}}")
                .build()
        def decoder = newWohnbauflaecheDecoder(profile)
        def xml = """<adv:AX_Wohnbauflaeche xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                gml:id="DENW36AL2mD000Cg">
              <adv:modellart>
                <adv:AA_Modellart>
                  <adv:advStandardModell>DLKM</adv:advStandardModell>
                </adv:AA_Modellart>
              </adv:modellart>
              <adv:modellart>
                <adv:AA_Modellart>
                  <adv:advStandardModell>DLKM</adv:advStandardModell>
                </adv:AA_Modellart>
                <adv:AA_Modellart>
                  <adv:sonstigesModell xlink:href="https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_WeitereModellart/NWABK"/>
                </adv:AA_Modellart>
              </adv:modellart>
            </adv:AX_Wohnbauflaeche>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        def matPath = ["mat"]
        indicesOfTokenAtPath(tokens, FeatureTokenType.ARRAY, matPath).size() == 1
        indicesOfTokenAtPath(tokens, FeatureTokenType.ARRAY_END, matPath).size() == 1
        indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT, matPath).size() == 3
        indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT_END, matPath).size() == 3
    }

    def 'GML array property shape: a second peer inside a non-array OBJECT_PROPERTY is reported as a schema/wire mismatch'() {
        given:
        // gmk in axFlurstueckWithGemarkungSchema is a single-valued OBJECT (not OBJECT_ARRAY); a
        // second peer AX_Gemarkung_Schluessel inside one <adv:gemarkung> is a genuine wire/schema
        // mismatch and must still be reported.
        def decoder = newDecoder(axFlurstueckWithGemarkungSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:gemarkung>
                <adv:AX_Gemarkung_Schluessel>
                  <adv:land>05</adv:land>
                </adv:AX_Gemarkung_Schluessel>
                <adv:AX_Gemarkung_Schluessel>
                  <adv:land>06</adv:land>
                </adv:AX_Gemarkung_Schluessel>
              </adv:gemarkung>
            </adv:AX_Flurstueck>"""

        when:
        runDecoder(decoder, xml)

        then:
        def e = thrown(Exception)
        def msg = rootCauseMessage(e)
        msg.contains("gmk")
        msg.contains("single-valued")
    }

    /**
     * Schema with a nested OBJECT property whose body holds a VALUE_ARRAY {@code tag} and a
     * scalar sibling {@code lab}. Used to exercise per-level ARRAY bracketing inside an
     * OBJECT_ELEMENT: the nested ARRAY pair must sit at the {@code [ngo, tag]} path, and the
     * sibling scalar {@code lab} at {@code [ngo, lab]}.
     */
    static FeatureSchema axFlurstueckWithNestedArraySchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("ngo", new ImmutableFeatureSchema.Builder()
                        .sourcePath("ngo")
                        .type(SchemaBase.Type.OBJECT)
                        .objectType("NestedObj")
                        .alias("nested")
                        .putProperties2("tag", new ImmutableFeatureSchema.Builder()
                                .sourcePath("[id=rid]ngo__tag/tag")
                                .type(SchemaBase.Type.VALUE_ARRAY)
                                .valueType(SchemaBase.Type.STRING)
                                .alias("tags"))
                        .putProperties2("lab", new ImmutableFeatureSchema.Builder()
                                .sourcePath("lab")
                                .type(SchemaBase.Type.STRING)
                                .alias("label")))
                .putProperties2("fsk", new ImmutableFeatureSchema.Builder()
                        .sourcePath("fsk")
                        .type(SchemaBase.Type.STRING)
                        .alias("flurstueckskennzeichen"))
                .build()
    }

    def 'nested VALUE_ARRAY inside an OBJECT property emits ARRAY/ARRAY_END at the nested path'() {
        given:
        // The two <adv:tags> peers sit inside an OBJECT (nested → NestedObj). The decoder must
        // open ARRAY at the nested path [ngo, tag] and close it before the sibling <adv:label>
        // emits — the bracketing is per nesting level, not just at the feature root.
        def decoder = newDecoder(axFlurstueckWithNestedArraySchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:nested>
                <adv:NestedObj>
                  <adv:tags>t1</adv:tags>
                  <adv:tags>t2</adv:tags>
                  <adv:label>L</adv:label>
                </adv:NestedObj>
              </adv:nested>
              <adv:flurstueckskennzeichen>code</adv:flurstueckskennzeichen>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        // Exactly one ARRAY pair, anchored at the nested path.
        indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY, ["ngo", "tag"]) >= 0
        indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY_END, ["ngo", "tag"]) >= 0
        tokens.count { it == FeatureTokenType.ARRAY } == 1
        tokens.count { it == FeatureTokenType.ARRAY_END } == 1
        // Both members emit inside the brackets.
        def arrayIdx = tokens.findIndexOf { it == FeatureTokenType.ARRAY }
        def arrayEndIdx = tokens.findIndexOf { it == FeatureTokenType.ARRAY_END }
        def t1Idx = tokens.findIndexOf { it == "t1" }
        def t2Idx = tokens.findIndexOf { it == "t2" }
        arrayIdx < t1Idx && t1Idx < arrayEndIdx
        arrayIdx < t2Idx && t2Idx < arrayEndIdx
        // Sibling scalar inside the same nested object lands at [ngo, lab], after the array closes.
        valueAtPath(tokens, ["ngo", "lab"]) == "L"
        tokens.indexOf("L") > arrayEndIdx
        // Root-level sibling lands at its own depth-1 path.
        valueAtPath(tokens, ["fsk"]) == "code"
    }

    def 'nested array still open at OBJECT_ELEMENT end closes before the OBJECT_END at the outer path'() {
        given:
        // The VALUE_ARRAY <adv:tags> is the *last* child of the nested object — no following
        // sibling triggers the close. The OBJECT_ELEMENT pop must still emit ARRAY_END (at the
        // inner path) before OBJECT_END (at the outer path) so the bracketing remains balanced.
        def decoder = newDecoder(axFlurstueckWithNestedArraySchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:nested>
                <adv:NestedObj>
                  <adv:label>L</adv:label>
                  <adv:tags>t1</adv:tags>
                  <adv:tags>t2</adv:tags>
                </adv:NestedObj>
              </adv:nested>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.count { it == FeatureTokenType.ARRAY } == 1
        tokens.count { it == FeatureTokenType.ARRAY_END } == 1
        def arrayEndIdx = indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY_END, ["ngo", "tag"])
        def objectEndIdx = indexOfTokenAtPath(tokens, FeatureTokenType.OBJECT_END, ["ngo"])
        arrayEndIdx >= 0
        objectEndIdx >= 0
        arrayEndIdx < objectEndIdx
    }

    /**
     * Schema with a nested OBJECT_ARRAY: outer OBJECT {@code ngo} contains the array {@code chi}
     * of {@code Child} objects with scalar {@code nam}/{@code val} children. Exercises ARRAY +
     * per-peer OBJECT bracketing at a non-root level.
     */
    static FeatureSchema axFlurstueckWithNestedObjectArraySchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("ngo", new ImmutableFeatureSchema.Builder()
                        .sourcePath("ngo")
                        .type(SchemaBase.Type.OBJECT)
                        .objectType("NestedObj")
                        .alias("nested")
                        .putProperties2("chi", new ImmutableFeatureSchema.Builder()
                                .sourcePath("[id=rid]ngo__chi")
                                .type(SchemaBase.Type.OBJECT_ARRAY)
                                .objectType("Child")
                                .alias("children")
                                .putProperties2("nam", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("nam")
                                        .type(SchemaBase.Type.STRING)
                                        .alias("name"))
                                .putProperties2("val", new ImmutableFeatureSchema.Builder()
                                        .sourcePath("val")
                                        .type(SchemaBase.Type.STRING)
                                        .alias("value"))))
                .build()
    }

    def 'nested OBJECT_ARRAY inside an OBJECT property emits one ARRAY pair with per-peer OBJECT pairs'() {
        given:
        def decoder = newDecoder(axFlurstueckWithNestedObjectArraySchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:nested>
                <adv:NestedObj>
                  <adv:children><adv:Child><adv:name>a</adv:name><adv:value>1</adv:value></adv:Child></adv:children>
                  <adv:children><adv:Child><adv:name>b</adv:name><adv:value>2</adv:value></adv:Child></adv:children>
                </adv:NestedObj>
              </adv:nested>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        // One ARRAY pair at the nested array path.
        tokens.count { it == FeatureTokenType.ARRAY } == 1
        tokens.count { it == FeatureTokenType.ARRAY_END } == 1
        indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY, ["ngo", "chi"]) >= 0
        indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY_END, ["ngo", "chi"]) >= 0
        // Two per-peer OBJECT pairs sit between the brackets, at the nested array path.
        indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT, ["ngo", "chi"]).size() == 2
        indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT_END, ["ngo", "chi"]).size() == 2
        // Scalar children land at the per-peer path.
        valueAtPath(tokens, ["ngo", "chi", "nam"]) == "a"
        valueAtPath(tokens, ["ngo", "chi", "val"]) == "1"
    }

    /**
     * Schema with a geometry property inside a nested OBJECT, used to exercise nested-geometry
     * decoding: the GML geometry subtree must decode regardless of nesting depth and the
     * resulting Geometry token must arrive at the nested property's path.
     */
    static FeatureSchema axFlurstueckWithNestedGeometrySchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("ngo", new ImmutableFeatureSchema.Builder()
                        .sourcePath("ngo")
                        .type(SchemaBase.Type.OBJECT)
                        .objectType("NestedObj")
                        .alias("nested")
                        .putProperties2("pos", new ImmutableFeatureSchema.Builder()
                                .sourcePath("pos")
                                .type(SchemaBase.Type.GEOMETRY)
                                .geometryType(GeometryType.POINT)
                                .alias("position")))
                .build()
    }

    def 'a geometry property inside a nested OBJECT decodes and emits at the nested path'() {
        given:
        def decoder = newDecoder(axFlurstueckWithNestedGeometrySchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:nested>
                <adv:NestedObj>
                  <adv:position>
                    <gml:Point gml:id="p1" srsName="http://www.opengis.net/def/crs/EPSG/0/25832">
                      <gml:pos>363609.477 5614790.107</gml:pos>
                    </gml:Point>
                  </adv:position>
                </adv:NestedObj>
              </adv:nested>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        def geomTokens = tokens.findAll { it instanceof Geometry } as List<Geometry>
        geomTokens.size() == 1
        geomTokens[0].type == GeometryType.POINT
        pathBeforeGeometry(tokens) == ["ngo", "pos"]
    }

    /**
     * AX_Flurstueck slice for the valueWrap reverse-mapping checks. {@code lzi_beg} mirrors
     * the AA_Objekt {@code lebenszeitintervall} property: a DATETIME with SQL column {@code
     * lzi__beg}, label {@code lebenszeitintervall_beginnt} and alias {@code
     * lebenszeitintervall}. The encoder wraps the scalar value in {@code
     * <AA_Lebenszeitintervall><beginnt>…</beginnt></AA_Lebenszeitintervall>}, matching the
     * {@code valueWrap} example in {@code GmlConfiguration}.
     */
    static FeatureSchema axFlurstueckWithLifeCycleSchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("lzi_beg", new ImmutableFeatureSchema.Builder()
                        .sourcePath("lzi__beg")
                        .type(SchemaBase.Type.DATETIME)
                        .alias("lebenszeitintervall"))
                .putProperties2("fsk", new ImmutableFeatureSchema.Builder()
                        .sourcePath("fsk")
                        .type(SchemaBase.Type.STRING)
                        .alias("flurstueckskennzeichen"))
                .build()
    }

    def 'valueWrap reverse-maps a wrapped scalar back to the property source path'() {
        given:
        // Encoder shape: <adv:lebenszeitintervall> (alias-named property element) wraps an
        // <adv:AA_Lebenszeitintervall><adv:beginnt>…</adv:beginnt></adv:AA_Lebenszeitintervall>
        // chain around the scalar. Reverse mapping must surface the inner text at lzi__beg.
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .putValueWrap("lzi_beg", ["AA_Lebenszeitintervall", "beginnt"])
                .build()
        def decoder = newDecoder(axFlurstueckWithLifeCycleSchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:lebenszeitintervall>
                <adv:AA_Lebenszeitintervall>
                  <adv:beginnt>2010-09-14T11:54:36Z</adv:beginnt>
                </adv:AA_Lebenszeitintervall>
              </adv:lebenszeitintervall>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["lzi_beg"]) == "2010-09-14T11:54:36Z"
    }

    def 'a sibling after a valueWrap chain still resolves at its own source path'() {
        given:
        // Path tracker must shorten back to the feature root once the wrapper chain closes, so
        // the following <adv:flurstueckskennzeichen> sibling lands at fsk, not at a stale path
        // left behind by the inner <adv:beginnt> wrapper.
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .putValueWrap("lzi_beg", ["AA_Lebenszeitintervall", "beginnt"])
                .build()
        def decoder = newDecoder(axFlurstueckWithLifeCycleSchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:lebenszeitintervall>
                <adv:AA_Lebenszeitintervall>
                  <adv:beginnt>2010-09-14T11:54:36Z</adv:beginnt>
                </adv:AA_Lebenszeitintervall>
              </adv:lebenszeitintervall>
              <adv:flurstueckskennzeichen>05334001001000010001__</adv:flurstueckskennzeichen>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["lzi_beg"]) == "2010-09-14T11:54:36Z"
        valueAtPath(tokens, ["fsk"]) == "05334001001000010001__"
    }

    def 'an unconfigured wrapped scalar is not surfaced (no permissive fallback)'() {
        given:
        // valueWrap is gated on configuration: without an entry, wrapper elements inside a
        // VALUE_PROPERTY are treated as unknown descendants and the inner text is not emitted.
        // This matches the encoder side, which only wraps when the option is set.
        def decoder = newDecoder(axFlurstueckWithLifeCycleSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:lebenszeitintervall>
                <adv:AA_Lebenszeitintervall>
                  <adv:beginnt>2010-09-14T11:54:36Z</adv:beginnt>
                </adv:AA_Lebenszeitintervall>
              </adv:lebenszeitintervall>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        !tokens.contains("2010-09-14T11:54:36Z")
    }

    static final String GMD_NS = "http://www.isotc211.org/2005/gmd"

    static final Map<String, String> NAMESPACES_WITH_GMD = [
            "adv": ADV_NS,
            "gml": "http://www.opengis.net/gml/3.2",
            "xlink": "http://www.w3.org/1999/xlink",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "gmd": GMD_NS,
    ]

    static final List<QName> PUNKTORTAU_FEATURE_TYPES = [new QName(ADV_NS, "AX_PunktortAU")]

    /**
     * AX_PunktortAU slice with the full {@code qualitaetsangaben} (qag) tree from the AdV NAS
     * schema: qag(AX_DQPunktort) → {dpl(LI_Lineage) → prs(LI_ProcessStep, OBJECT_ARRAY) → {des,
     * dat, pro(CI_ResponsibleParty)→{org, ind, rol}, src(LI_Source)→des}, gst}. Every nested
     * OBJECT is transparent (no {@code sourcePath}), so each leaf emits at the feature-root
     * path with its {@code qag__*} SQL column name.
     */
    static FeatureSchema axPunktortAuWithQagSchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_punktortau")
                .sourcePath("/o14003")
                .type(SchemaBase.Type.OBJECT)
                .objectType("AX_PunktortAU")
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("qag", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.OBJECT)
                        .objectType("AX_DQPunktort")
                        .alias("qualitaetsangaben")
                        .putProperties2("dpl", new ImmutableFeatureSchema.Builder()
                                .type(SchemaBase.Type.OBJECT)
                                .objectType("LI_Lineage")
                                .alias("herkunft")
                                .putProperties2("prs", new ImmutableFeatureSchema.Builder()
                                        .type(SchemaBase.Type.OBJECT_ARRAY)
                                        .objectType("LI_ProcessStep")
                                        .alias("processStep")
                                        .putProperties2("des", new ImmutableFeatureSchema.Builder()
                                                .sourcePath("qag__dpl_des")
                                                .type(SchemaBase.Type.STRING)
                                                .alias("description"))
                                        .putProperties2("dat", new ImmutableFeatureSchema.Builder()
                                                .sourcePath("qag__dpl_prs_dat")
                                                .type(SchemaBase.Type.DATETIME)
                                                .alias("dateTime"))
                                        .putProperties2("pro", new ImmutableFeatureSchema.Builder()
                                                .type(SchemaBase.Type.OBJECT)
                                                .objectType("CI_ResponsibleParty")
                                                .alias("processor")
                                                .putProperties2("org", new ImmutableFeatureSchema.Builder()
                                                        .sourcePath("qag__dpl_prs_pro_resp_org")
                                                        .type(SchemaBase.Type.STRING)
                                                        .alias("organisationName"))
                                                .putProperties2("ind", new ImmutableFeatureSchema.Builder()
                                                        .sourcePath("qag__dpl_prs_pro_resp_ind")
                                                        .type(SchemaBase.Type.STRING)
                                                        .alias("individualName"))
                                                .putProperties2("rol", new ImmutableFeatureSchema.Builder()
                                                        .sourcePath("qag__dpl_prs_pro_resp_rol_cdv")
                                                        .type(SchemaBase.Type.STRING)
                                                        .alias("role")
                                                        .constraints(new ImmutableSchemaConstraints.Builder()
                                                                .codelist("CI_RoleCode")
                                                                .build())))
                                        .putProperties2("src", new ImmutableFeatureSchema.Builder()
                                                .type(SchemaBase.Type.OBJECT)
                                                .objectType("LI_Source")
                                                .alias("source")
                                                .putProperties2("des", new ImmutableFeatureSchema.Builder()
                                                        .sourcePath("qag__dpl_prs_src")
                                                        .type(SchemaBase.Type.STRING)
                                                        .alias("description")))))
                        .putProperties2("gst", new ImmutableFeatureSchema.Builder()
                                .sourcePath("qag__gst")
                                .type(SchemaBase.Type.STRING)
                                .alias("genauigkeitsstufe")
                                .constraints(new ImmutableSchemaConstraints.Builder()
                                        .codelist("AX_Genauigkeitsstufe_Punktort")
                                        .build())))
                .build()
    }

    static FeatureTokenDecoderSimple<byte[], FeatureSchema, SchemaMapping, FeatureEventHandlerSimple.ModifiableContext<FeatureSchema, SchemaMapping>> newPunktortAuDecoder(
            FeatureTokenDecoderGmlInputProfile profile) {
        def schema = axPunktortAuWithQagSchema()
        new FeatureTokenDecoderGml(
                NAMESPACES_WITH_GMD,
                PUNKTORTAU_FEATURE_TYPES,
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
                profile)
    }

    /**
     * NAS-shaped namespace profile: AX_PunktortAU / AX_DQPunktort fall back to the default adv
     * namespace, the four ISO 19115 object types (LI_Lineage, LI_ProcessStep, LI_Source,
     * CI_ResponsibleParty) are in gmd. The {@code valueWrap} entries declare the two adv-
     * namespaced content-carrying wrappers that the encoder writes around two of the
     * LI_ProcessStep scalar children; gmd/gco wrappers (gco:DateTime, gco:CharacterString,
     * gmd:CI_RoleCode) are auto-detected by the decoder and need no entry here.
     */
    static FeatureTokenDecoderGmlInputProfile nasNamespaceProfile() {
        ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .defaultNamespace("adv")
                .putObjectTypeNamespaces("LI_Lineage", "gmd")
                .putObjectTypeNamespaces("LI_ProcessStep", "gmd")
                .putObjectTypeNamespaces("LI_Source", "gmd")
                .putObjectTypeNamespaces("CI_ResponsibleParty", "gmd")
                .putValueWrap("qag.dpl.prs.des", ["AX_LI_ProcessStep_Punktort_Description"])
                .putValueWrap("qag.dpl.prs.src.des", ["AX_Datenerhebung_Punktort"])
                .build()
    }

    def 'defaultNamespace constrains property elements to the configured namespace URI'() {
        given:
        // With defaultNamespace set, property elements on the wire are required to live in the
        // configured namespace; same-localName elements in a different namespace must not be
        // matched against the schema property.
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .defaultNamespace("adv")
                .build()
        def decoder = newDecoder(axFlurstueckSchema(), profile)
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:other="http://example.org/other"
                gml:id="DENW36AL10000XYZ">
              <adv:flurstueckskennzeichen>05334001001000010001__</adv:flurstueckskennzeichen>
              <other:flurstueckskennzeichen>NOT_IN_ADV_NS</other:flurstueckskennzeichen>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        // Only the ADV-namespaced element resolves; the same-localName element in another
        // namespace must be skipped.
        valueAtPath(tokens, ["fsk"]) == "05334001001000010001__"
        !tokens.contains("NOT_IN_ADV_NS")
    }

    def 'the full qag tree resolves every leaf across adv/gmd/gco'() {
        given:
        // AX_PunktortAU qualitaetsangaben (qag) wire form: the outer wrappers
        // (qualitaetsangaben, AX_DQPunktort, herkunft) live in the default adv namespace; the
        // ISO 19115 chain (LI_Lineage / processStep / LI_ProcessStep / CI_ResponsibleParty /
        // LI_Source) is in gmd, with leaf values wrapped in adv-namespaced or gmd/gco
        // content-carrying objects:
        //   - description (LI_ProcessStep) wraps <adv:AX_LI_ProcessStep_Punktort_Description>
        //     — adv namespace, requires explicit valueWrap config
        //   - dateTime wraps <gco:DateTime> — gmd/gco auto-detected
        //   - organisationName wraps <gco:CharacterString> — auto-detected
        //   - role wraps <gmd:CI_RoleCode codeListValue=…> — auto-detected; text content used
        //     (rol has a codelist constraint, but the gmd text path wins; see the focused test below)
        //   - source/description wraps <adv:AX_Datenerhebung_Punktort> — explicit valueWrap
        // The sibling <adv:genauigkeitsstufe> at AX_DQPunktort level exercises a scalar that
        // sits next to the deep dpl subtree.
        def decoder = newPunktortAuDecoder(nasNamespaceProfile())
        def xml = """<adv:AX_PunktortAU xmlns:adv="${ADV_NS}"
                xmlns:gmd="${GMD_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW31AL10000ABC">
              <adv:qualitaetsangaben>
                <adv:AX_DQPunktort>
                  <adv:herkunft>
                    <gmd:LI_Lineage>
                      <gmd:processStep>
                        <gmd:LI_ProcessStep>
                          <gmd:description>
                            <adv:AX_LI_ProcessStep_Punktort_Description>Erhebung</adv:AX_LI_ProcessStep_Punktort_Description>
                          </gmd:description>
                          <gmd:dateTime>
                            <gco:DateTime xmlns:gco="http://www.isotc211.org/2005/gco">2017-07-20T11:20:04Z</gco:DateTime>
                          </gmd:dateTime>
                          <gmd:processor>
                            <gmd:CI_ResponsibleParty>
                              <gmd:organisationName>
                                <gco:CharacterString xmlns:gco="http://www.isotc211.org/2005/gco">Amt für Bodenmanagement und Geoinformation Bonn</gco:CharacterString>
                              </gmd:organisationName>
                              <gmd:role>
                                <gmd:CI_RoleCode codeList="http://www.isotc211.org/2005/gmd#CI_RoleCode" codeListValue="processor">processor</gmd:CI_RoleCode>
                              </gmd:role>
                            </gmd:CI_ResponsibleParty>
                          </gmd:processor>
                          <gmd:source>
                            <gmd:LI_Source>
                              <gmd:description>
                                <adv:AX_Datenerhebung_Punktort>1000</adv:AX_Datenerhebung_Punktort>
                              </gmd:description>
                            </gmd:LI_Source>
                          </gmd:source>
                        </gmd:LI_ProcessStep>
                      </gmd:processStep>
                    </gmd:LI_Lineage>
                  </adv:herkunft>
                  <adv:genauigkeitsstufe>2100</adv:genauigkeitsstufe>
                </adv:AX_DQPunktort>
              </adv:qualitaetsangaben>
            </adv:AX_PunktortAU>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        // qag and its descendants are all transparent OBJECTs, so every leaf emits at the
        // feature root with its qag__* column name.
        valueAtPath(tokens, ["qag", "dpl", "prs", "des"]) == "Erhebung"
        valueAtPath(tokens, ["qag", "dpl", "prs", "dat"]) == "2017-07-20T11:20:04Z"
        valueAtPath(tokens, ["qag", "dpl", "prs", "pro", "org"]) == "Amt für Bodenmanagement und Geoinformation Bonn"
        valueAtPath(tokens, ["qag", "dpl", "prs", "pro", "rol"]) == "processor"
        valueAtPath(tokens, ["qag", "dpl", "prs", "src", "des"]) == "1000"
        valueAtPath(tokens, ["qag", "gst"]) == "2100"
    }

    def 'a codelist-constrained role still decodes from the gmd:CI_RoleCode text, not as an xlink:href'() {
        given:
        // Regression guard for the ISO 19139 codelist encoding (GmlConfiguration#codeListUriTemplateIso19139).
        // The `rol` property carries a `codelist` constraint, which makes the decoder treat it as a
        // codelist property eligible for xlink:href routing. The ISO 19139 wire, however, carries the
        // value in the gmd:CI_RoleCode text content (and the codeListValue attribute), with no
        // xlink:href on the property element — so the codelist routing finds no href and the gmd
        // value-wrapper text is used. The codeList URI must not leak into the decoded value.
        def decoder = newPunktortAuDecoder(nasNamespaceProfile())
        def xml = """<adv:AX_PunktortAU xmlns:adv="${ADV_NS}"
                xmlns:gmd="${GMD_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW31AL10000ABC">
              <adv:qualitaetsangaben>
                <adv:AX_DQPunktort>
                  <adv:herkunft>
                    <gmd:LI_Lineage>
                      <gmd:processStep>
                        <gmd:LI_ProcessStep>
                          <gmd:processor>
                            <gmd:CI_ResponsibleParty>
                              <gmd:role>
                                <gmd:CI_RoleCode codeList="https://schemas.isotc211.org/19139/resources/codelists/gmxCodelists.xml/gmxCodelists.xml#CI_RoleCode" codeListValue="processor">processor</gmd:CI_RoleCode>
                              </gmd:role>
                            </gmd:CI_ResponsibleParty>
                          </gmd:processor>
                        </gmd:LI_ProcessStep>
                      </gmd:processStep>
                    </gmd:LI_Lineage>
                  </adv:herkunft>
                </adv:AX_DQPunktort>
              </adv:qualitaetsangaben>
            </adv:AX_PunktortAU>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["qag", "dpl", "prs", "pro", "rol"]) == "processor"
        !tokens.contains("https://schemas.isotc211.org/19139/resources/codelists/gmxCodelists.xml/gmxCodelists.xml#CI_RoleCode")
    }

    def 'LI_ProcessStep children written in the wrong namespace are skipped'() {
        given:
        // Sanity check: with objectTypeNamespaces[LI_ProcessStep] = gmd, an LI_ProcessStep
        // child written in adv (instead of gmd) must be dropped rather than mis-matched. The
        // gmd-namespaced sibling next to it still resolves, proving the check is per-element
        // and does not poison the rest of the subtree.
        def decoder = newPunktortAuDecoder(nasNamespaceProfile())
        def xml = """<adv:AX_PunktortAU xmlns:adv="${ADV_NS}"
                xmlns:gmd="${GMD_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW31AL10000ABC">
              <adv:qualitaetsangaben>
                <adv:AX_DQPunktort>
                  <adv:herkunft>
                    <gmd:LI_Lineage>
                      <gmd:processStep>
                        <gmd:LI_ProcessStep>
                          <adv:dateTime>
                            <gco:DateTime xmlns:gco="http://www.isotc211.org/2005/gco">WRONG_NS</gco:DateTime>
                          </adv:dateTime>
                          <gmd:source>
                            <gmd:LI_Source>
                              <gmd:description>
                                <adv:AX_Datenerhebung_Punktort>1000</adv:AX_Datenerhebung_Punktort>
                              </gmd:description>
                            </gmd:LI_Source>
                          </gmd:source>
                        </gmd:LI_ProcessStep>
                      </gmd:processStep>
                    </gmd:LI_Lineage>
                  </adv:herkunft>
                </adv:AX_DQPunktort>
              </adv:qualitaetsangaben>
            </adv:AX_PunktortAU>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        !tokens.contains("WRONG_NS")
        valueAtPath(tokens, ["qag", "dpl", "prs", "dat"]) == null
        valueAtPath(tokens, ["qag", "dpl", "prs", "src", "des"]) == "1000"
    }

    def 'with no namespace configuration matching is by local name alone'() {
        given:
        // No defaultNamespace, no objectTypeNamespaces — the existing fixture-style behaviour
        // applies: the wire URI is ignored and the same-localName element in any namespace
        // matches. This is what the existing tests have always relied on.
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:other="http://example.org/other"
                gml:id="DENW36AL10000XYZ">
              <other:flurstueckskennzeichen>matched-by-local-name</other:flurstueckskennzeichen>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["fsk"]) == "matched-by-local-name"
    }

    def 'xsi:type on a property element is rejected with a clear error'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                gml:id="DENW36AL10000XYZ">
              <adv:flurstueckskennzeichen xsi:type="xsd:string">05334001001000010001__</adv:flurstueckskennzeichen>
            </adv:AX_Flurstueck>"""

        when:
        runDecoder(decoder, xml)

        then:
        def e = thrown(Exception)
        rootCauseMessage(e).contains("xsi:type")
        rootCauseMessage(e).contains("flurstueckskennzeichen")
    }

    def 'xsi:type on the feature root is rejected with a clear error'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:type="adv:AX_Flurstueck_Ext"
                gml:id="DENW36AL10000XYZ"/>"""

        when:
        runDecoder(decoder, xml)

        then:
        def e = thrown(Exception)
        rootCauseMessage(e).contains("xsi:type")
        rootCauseMessage(e).contains("AX_Flurstueck")
    }

    def 'nilReason on a property element is silently dropped'() {
        given:
        // Verifies that an unqualified nilReason attribute, with or without xsi:nil, does not
        // leak into the token stream — the encoder never emits it and the decoder mirrors that.
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:flurstueckskennzeichen nilReason="other">05334001001000010001__</adv:flurstueckskennzeichen>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["fsk"]) == "05334001001000010001__"
        !tokens.contains("other")
    }

    /**
     * Made-up subtype-discrimination shape: AX_Flurstueck instances split into two variants with
     * different qualified element names on the wire, distinguished by a single STRING property
     * {@code art} on the feature root. No production NAS feature type uses this pattern (NAS
     * encodes subtype variation via codelist properties rather than element-name variation), so
     * the fixture is illustrative rather than data-driven.
     */
    static FeatureSchema axFlurstueckVariantSchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .objectType("AX_Flurstueck")
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("art", new ImmutableFeatureSchema.Builder()
                        .sourcePath("art")
                        .type(SchemaBase.Type.STRING)
                        .alias("art"))
                .build()
    }

    static FeatureTokenDecoderGmlInputProfile variableNameProfile() {
        ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .putVariableObjectElementNames("AX_Flurstueck",
                        new ImmutableVariableObjectName.Builder()
                                .property("art")
                                .putMapping("adv:AX_FlurstueckMitArt1", "art1")
                                .putMapping("adv:AX_FlurstueckMitArt2", "art2")
                                .build())
                .build()
    }

    def 'variableObjectElementNames accepts a varying feature root element and emits the discriminator value'() {
        given:
        def decoder = newDecoder(axFlurstueckVariantSchema(), variableNameProfile())
        def xml = """<adv:AX_FlurstueckMitArt1 xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ"/>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.first() == FeatureTokenType.INPUT
        tokens.last() == FeatureTokenType.INPUT_END
        tokens.count { it == FeatureTokenType.FEATURE } == 1
        valueAtPath(tokens, ["oid"]) == "DENW36AL10000XYZ"
        valueAtPath(tokens, ["art"]) == "art1"
    }

    def 'variableObjectElementNames resolves the wire element against the canonical namespace prefix'() {
        given:
        // Wire uses a different prefix declaration ("a") for the same ADV namespace URI; the
        // decoder normalises via its constructor namespace map so the mapping key "adv:..."
        // still matches.
        def decoder = newDecoder(axFlurstueckVariantSchema(), variableNameProfile())
        def xml = """<a:AX_FlurstueckMitArt2 xmlns:a="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ"/>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["art"]) == "art2"
    }

    def 'an unconfigured variable wire element name is rejected as multi-feature'() {
        given:
        def decoder = newDecoder(axFlurstueckVariantSchema(), variableNameProfile())
        def xml = """<adv:AX_FlurstueckMitArt9 xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ"/>"""

        when:
        runDecoder(decoder, xml)

        then:
        def e = thrown(Exception)
        rootCauseMessage(e).contains("Multi-feature ingest is not supported")
    }

    /**
     * Same shape as {@link #axFlurstueckWithGemarkungSchema} but the nested {@code gmk}
     * (AX_Gemarkung_Schluessel) OBJECT carries an additional STRING discriminator child {@code
     * art}. Used together with a profile that maps two variant qualified wire element names for
     * {@code AX_Gemarkung_Schluessel}: the decoder substitutes the discriminator value
     * accordingly. Illustrative rather than data-driven — production NAS encodes subtype variation
     * via codelist properties, not nested element-name variation.
     */
    static FeatureSchema axFlurstueckWithGemarkungVariantSchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_flurstueck")
                .sourcePath("/o11001")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("gmk", new ImmutableFeatureSchema.Builder()
                        .sourcePath("gmk")
                        .type(SchemaBase.Type.OBJECT)
                        .objectType("AX_Gemarkung_Schluessel")
                        .alias("gemarkung")
                        .putProperties2("art", new ImmutableFeatureSchema.Builder()
                                .sourcePath("art")
                                .type(SchemaBase.Type.STRING)
                                .alias("art"))
                        .putProperties2("lan", new ImmutableFeatureSchema.Builder()
                                .sourcePath("lan")
                                .type(SchemaBase.Type.STRING)
                                .alias("land"))
                        .putProperties2("gmn", new ImmutableFeatureSchema.Builder()
                                .sourcePath("gmn")
                                .type(SchemaBase.Type.STRING)
                                .alias("gemarkungsnummer")))
                .build()
    }

    static FeatureTokenDecoderGmlInputProfile nestedVariableNameProfile() {
        ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .putVariableObjectElementNames("AX_Gemarkung_Schluessel",
                        new ImmutableVariableObjectName.Builder()
                                .property("art")
                                .putMapping("adv:AX_GemarkungMitArt1", "art1")
                                .putMapping("adv:AX_GemarkungMitArt2", "art2")
                                .build())
                .build()
    }

    def 'nested variableObjectElementNames emits the discriminator value at the nested OBJECT path'() {
        given:
        def decoder = newDecoder(axFlurstueckWithGemarkungVariantSchema(), nestedVariableNameProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:gemarkung>
                <adv:AX_GemarkungMitArt1>
                  <adv:land>05</adv:land>
                  <adv:gemarkungsnummer>4320</adv:gemarkungsnummer>
                </adv:AX_GemarkungMitArt1>
              </adv:gemarkung>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["gmk", "art"]) == "art1"
        valueAtPath(tokens, ["gmk", "lan"]) == "05"
        valueAtPath(tokens, ["gmk", "gmn"]) == "4320"
    }

    def 'nested variableObjectElementNames resolves the wire element through the canonical namespace prefix'() {
        given:
        // Wire uses 'a' as a second prefix for the ADV namespace URI; the decoder normalises the
        // wire qualified name through its constructor namespace map so the configured mapping
        // key "adv:AX_GemarkungMitArt2" still matches.
        def decoder = newDecoder(axFlurstueckWithGemarkungVariantSchema(), nestedVariableNameProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:a="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:gemarkung>
                <a:AX_GemarkungMitArt2>
                  <adv:land>05</adv:land>
                </a:AX_GemarkungMitArt2>
              </adv:gemarkung>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["gmk", "art"]) == "art2"
    }

    def 'a nested OBJECT_ELEMENT not matching a configured variant emits no discriminator but still decodes the children'() {
        given:
        // The canonical AX_Gemarkung_Schluessel inner element is *not* in the variant mapping; the
        // decoder still treats it as the OBJECT_ELEMENT of <adv:gemarkung> (per GML's alternation
        // rule) and decodes its children. No synthetic discriminator value is produced.
        def decoder = newDecoder(axFlurstueckWithGemarkungVariantSchema(), nestedVariableNameProfile())
        def xml = """<adv:AX_Flurstueck xmlns:adv="${ADV_NS}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000XYZ">
              <adv:gemarkung>
                <adv:AX_Gemarkung_Schluessel>
                  <adv:land>05</adv:land>
                </adv:AX_Gemarkung_Schluessel>
              </adv:gemarkung>
            </adv:AX_Flurstueck>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["gmk", "art"]) == null
        valueAtPath(tokens, ["gmk", "lan"]) == "05"
    }

    def 'a wfs:FeatureCollection root is rejected as multi-feature'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), useAliasProfile())
        def xml = """<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"
                xmlns:adv="${ADV_NS}" xmlns:gml="http://www.opengis.net/gml/3.2">
              <wfs:member>
                <adv:AX_Flurstueck gml:id="DENW36AL10000XYZ"/>
              </wfs:member>
            </wfs:FeatureCollection>"""

        when:
        runDecoder(decoder, xml)

        then:
        def e = thrown(Exception)
        rootCauseMessage(e).contains("Multi-feature ingest is not supported")
    }

    /**
     * Profile mirroring the encoder's default collection wrapper around each feature:
     * {@code <sf:FeatureCollection><sf:featureMember><feature/></sf:featureMember></sf:FeatureCollection>}.
     */
    static FeatureTokenDecoderGmlInputProfile sfWrapperProfile() {
        ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .featureCollectionElementName("sf:FeatureCollection")
                .featureMemberElementName("sf:featureMember")
                .build()
    }

    def 'configured featureCollection/featureMember wrappers are descended and the inner feature is decoded'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), sfWrapperProfile())
        def xml = """<sf:FeatureCollection xmlns:sf="${SF_NS}"
                xmlns:adv="${ADV_NS}" xmlns:gml="http://www.opengis.net/gml/3.2">
              <sf:featureMember>
                <adv:AX_Flurstueck gml:id="DENW36AL10000XYZ">
                  <adv:flurstueckskennzeichen>05334001001000010001__</adv:flurstueckskennzeichen>
                </adv:AX_Flurstueck>
              </sf:featureMember>
            </sf:FeatureCollection>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.first() == FeatureTokenType.INPUT
        tokens.last() == FeatureTokenType.INPUT_END
        tokens.count { it == FeatureTokenType.FEATURE } == 1
        tokens.count { it == FeatureTokenType.FEATURE_END } == 1
        valueAtPath(tokens, ["oid"]) == "DENW36AL10000XYZ"
        valueAtPath(tokens, ["fsk"]) == "05334001001000010001__"
    }

    def 'configured featureMember wrapper alone is descended (no enclosing collection)'() {
        given:
        def profile = ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .featureMemberElementName("sf:featureMember")
                .build()
        def decoder = newDecoder(axFlurstueckSchema(), profile)
        def xml = """<sf:featureMember xmlns:sf="${SF_NS}"
                xmlns:adv="${ADV_NS}" xmlns:gml="http://www.opengis.net/gml/3.2">
              <adv:AX_Flurstueck gml:id="DENW36AL10000XYZ">
                <adv:flurstueckskennzeichen>05334001001000010001__</adv:flurstueckskennzeichen>
              </adv:AX_Flurstueck>
            </sf:featureMember>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.count { it == FeatureTokenType.FEATURE } == 1
        valueAtPath(tokens, ["fsk"]) == "05334001001000010001__"
    }

    def 'configured collection wrapper resolves the prefix via the constructor namespace map even with a different wire prefix'() {
        given:
        // Wire uses 'opf' for the same sf namespace URI; the decoder normalises through its
        // constructor namespace map so the profile's "sf:FeatureCollection" still matches.
        def decoder = newDecoder(axFlurstueckSchema(), sfWrapperProfile())
        def xml = """<opf:FeatureCollection xmlns:opf="${SF_NS}"
                xmlns:adv="${ADV_NS}" xmlns:gml="http://www.opengis.net/gml/3.2">
              <opf:featureMember>
                <adv:AX_Flurstueck gml:id="DENW36AL10000XYZ"/>
              </opf:featureMember>
            </opf:FeatureCollection>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        tokens.count { it == FeatureTokenType.FEATURE } == 1
        valueAtPath(tokens, ["oid"]) == "DENW36AL10000XYZ"
    }

    def 'a mismatching wrapper root with the collection option configured is rejected as multi-feature'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), sfWrapperProfile())
        // Wire root is wfs:FeatureCollection but the profile expects sf:FeatureCollection.
        def xml = """<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"
                xmlns:adv="${ADV_NS}" xmlns:gml="http://www.opengis.net/gml/3.2">
              <wfs:member>
                <adv:AX_Flurstueck gml:id="DENW36AL10000XYZ"/>
              </wfs:member>
            </wfs:FeatureCollection>"""

        when:
        runDecoder(decoder, xml)

        then:
        def e = thrown(Exception)
        rootCauseMessage(e).contains("Multi-feature ingest is not supported")
    }

    def 'a second feature inside the configured collection wrapper is rejected as multi-feature'() {
        given:
        def decoder = newDecoder(axFlurstueckSchema(), sfWrapperProfile())
        def xml = """<sf:FeatureCollection xmlns:sf="${SF_NS}"
                xmlns:adv="${ADV_NS}" xmlns:gml="http://www.opengis.net/gml/3.2">
              <sf:featureMember>
                <adv:AX_Flurstueck gml:id="DENW36AL10000XYZ"/>
              </sf:featureMember>
              <sf:featureMember>
                <adv:AX_Flurstueck gml:id="DENW36AL10000ABC"/>
              </sf:featureMember>
            </sf:FeatureCollection>"""

        when:
        runDecoder(decoder, xml)

        then:
        def e = thrown(Exception)
        rootCauseMessage(e).contains("Multi-feature ingest is not supported")
    }

    /**
     * Index of the first {@code token} whose path operand (the next list in the stream) equals
     * {@code targetPath}, or {@code -1} if no such token exists. Useful for asserting on
     * structural tokens (ARRAY, OBJECT, OBJECT_END, ARRAY_END) where the value-after-VALUE
     * helper does not apply.
     */
    private static int indexOfTokenAtPath(List<Object> tokens, FeatureTokenType token, List<String> targetPath) {
        for (int i = 0; i < tokens.size() - 1; i++) {
            if (tokens[i] == token && tokens.get(i + 1) == targetPath) {
                return i
            }
        }
        return -1
    }

    private static List<Integer> indicesOfTokenAtPath(List<Object> tokens, FeatureTokenType token, List<String> targetPath) {
        def indices = new ArrayList<Integer>()
        for (int i = 0; i < tokens.size() - 1; i++) {
            if (tokens[i] == token && tokens.get(i + 1) == targetPath) {
                indices.add(i)
            }
        }
        return indices
    }

    private static String valueAtPath(List<Object> tokens, List<String> targetPath) {
        for (int i = 0; i < tokens.size() - 2; i++) {
            if (tokens[i] != FeatureTokenType.VALUE) continue
            if (tokens.get(i + 1) == targetPath) {
                return tokens.get(i + 2) as String
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

    // -------------------------------------------------------------------------------------------
    // AX_Gebaeude end-to-end coverage. The profile exercises:
    //   - applicationNamespaces declaring the ADV URI under a non-default prefix ("aaa"), while the
    //     wire XML uses "adv" for the same URI → namespace lookup must match on URI, not prefix
    //   - defaultNamespace pointing at that "aaa" prefix
    //   - useAlias: true with mixed-namespace inner elements (adv + gmd/gco) reached via
    //     objectTypeNamespaces for the ISO 19115 object types
    //   - valueWrap entries keyed both by alias path (lebenszeitintervall) and by property-name
    //     path (qag.dpl.prs.des / qag.dpl.prs.src.des), which are the two key shapes the decoder
    //     recognises
    //   - codelistUriTemplate and featureRefTemplate so that xlink:href on adv:anlass and
    //     adv:hat is reduced to the bare value
    // -------------------------------------------------------------------------------------------

    static final String ADV_PREFIX_URI = ADV_NS
    static final String GCO_NS = "http://www.isotc211.org/2005/gco"

    static final Map<String, String> AX_GEBAEUDE_NAMESPACES = [
            "aaa": ADV_PREFIX_URI,
            "gmd": GMD_NS,
            "gco": GCO_NS,
            "gml": "http://www.opengis.net/gml/3.2",
            "xlink": "http://www.w3.org/1999/xlink",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "wfs": "http://www.opengis.net/wfs/2.0"
    ]

    /**
     * AX_Gebaeude slice covering every property in {@code src/test/resources/nas/AX_Gebaeude.xml}:
     * {@code oid} (gml:id) and {@code idn} (gml:identifier); the surface geometry {@code gpo}
     * (position); the simple STRING {@code gfk} (gebaeudefunktion); the DATETIME {@code lzi_beg}
     * (lebenszeitintervall) reached through an adv-namespaced valueWrap chain; the OBJECT_ARRAY
     * {@code mat} (modellart) with {@code stm} / {@code som} children; the codelist VALUE_ARRAY
     * {@code anl} (anlass); the FEATURE_REF {@code hat}; and the deep transparent OBJECT chain
     * {@code qag} (qualitaetsangaben) → {@code dpl} (LI_Lineage) → {@code prs}
     * (LI_ProcessStep, OBJECT_ARRAY) → {@code des} / {@code pro} (CI_ResponsibleParty →
     * {@code org} + {@code rol}) / {@code src} (LI_Source → {@code des}).
     */
    static FeatureSchema axGebaeudeSchema() {
        new ImmutableFeatureSchema.Builder()
                .name("ax_gebaeude")
                .sourcePath("/o31001")
                .type(SchemaBase.Type.OBJECT)
                .objectType("AX_Gebaeude")
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("idn", new ImmutableFeatureSchema.Builder()
                        .sourcePath("idn")
                        .type(SchemaBase.Type.STRING)
                        // Explicit gml: prefix on the alias pins the element to the GML namespace
                        // regardless of the profile's defaultNamespace.
                        .alias("gml:identifier"))
                .putProperties2("gpo", new ImmutableFeatureSchema.Builder()
                        .sourcePath("gpo")
                        .type(SchemaBase.Type.GEOMETRY)
                        .geometryType(GeometryType.CURVE_POLYGON)
                        .alias("position"))
                .putProperties2("gfk", new ImmutableFeatureSchema.Builder()
                        .sourcePath("gfk")
                        .type(SchemaBase.Type.STRING)
                        .alias("gebaeudefunktion"))
                .putProperties2("lzi_beg", new ImmutableFeatureSchema.Builder()
                        .sourcePath("lzi__beg")
                        .type(SchemaBase.Type.DATETIME)
                        .alias("lebenszeitintervall"))
                .putProperties2("mat", new ImmutableFeatureSchema.Builder()
                        .sourcePath("[id=rid]o31001__mat")
                        .type(SchemaBase.Type.OBJECT_ARRAY)
                        .objectType("AA_Modellart")
                        .alias("modellart")
                        .putProperties2("stm", new ImmutableFeatureSchema.Builder()
                                .sourcePath("stm")
                                .type(SchemaBase.Type.STRING)
                                .alias("advStandardModell"))
                        .putProperties2("som", new ImmutableFeatureSchema.Builder()
                                .sourcePath("som")
                                .type(SchemaBase.Type.STRING)
                                .alias("sonstigesModell")
                                .constraints(new ImmutableSchemaConstraints.Builder()
                                        .codelist("AA_WeitereModellart")
                                        .build())))
                .putProperties2("anl", new ImmutableFeatureSchema.Builder()
                        .sourcePath("[id=rid]o31001__anl/anl_href")
                        .type(SchemaBase.Type.VALUE_ARRAY)
                        .valueType(SchemaBase.Type.STRING)
                        .alias("anlass")
                        .constraints(new ImmutableSchemaConstraints.Builder()
                                .codelist("AA_Anlassart")
                                .build()))
                .putProperties2("qag", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.OBJECT)
                        .objectType("AX_DQMitDatenerhebung")
                        .alias("qualitaetsangaben")
                        .putProperties2("dpl", new ImmutableFeatureSchema.Builder()
                                .type(SchemaBase.Type.OBJECT)
                                .objectType("LI_Lineage")
                                .alias("herkunft")
                                .putProperties2("prs", new ImmutableFeatureSchema.Builder()
                                        .type(SchemaBase.Type.OBJECT_ARRAY)
                                        .objectType("LI_ProcessStep")
                                        .alias("processStep")
                                        .putProperties2("des", new ImmutableFeatureSchema.Builder()
                                                .sourcePath("qag__dpl_des")
                                                .type(SchemaBase.Type.STRING)
                                                .alias("description"))
                                        .putProperties2("pro", new ImmutableFeatureSchema.Builder()
                                                .type(SchemaBase.Type.OBJECT)
                                                .objectType("CI_ResponsibleParty")
                                                .alias("processor")
                                                .putProperties2("org", new ImmutableFeatureSchema.Builder()
                                                        .sourcePath("qag__dpl_prs_pro_resp_org")
                                                        .type(SchemaBase.Type.STRING)
                                                        .alias("organisationName"))
                                                .putProperties2("rol", new ImmutableFeatureSchema.Builder()
                                                        .sourcePath("qag__dpl_prs_pro_resp_rol_cdv")
                                                        .type(SchemaBase.Type.STRING)
                                                        .alias("role")))
                                        .putProperties2("src", new ImmutableFeatureSchema.Builder()
                                                .type(SchemaBase.Type.OBJECT)
                                                .objectType("LI_Source")
                                                .alias("source")
                                                .putProperties2("des", new ImmutableFeatureSchema.Builder()
                                                        .sourcePath("qag__dpl_prs_src")
                                                        .type(SchemaBase.Type.STRING)
                                                        .alias("description"))))))
                .putProperties2("hat", new ImmutableFeatureSchema.Builder()
                        .sourcePath("p3100131001")
                        .type(SchemaBase.Type.FEATURE_REF)
                        .valueType(SchemaBase.Type.STRING)
                        .alias("hat")
                        .refType("ax_gebaeude"))
                .build()
    }

    static FeatureTokenDecoderGmlInputProfile axGebaeudeProfile() {
        ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                .useAlias(true)
                .defaultNamespace("aaa")
                .putApplicationNamespaces("aaa", ADV_PREFIX_URI)
                .putApplicationNamespaces("gmd", GMD_NS)
                .putApplicationNamespaces("gco", GCO_NS)
                .putObjectTypeNamespaces("LI_Lineage", "gmd")
                .putObjectTypeNamespaces("LI_ProcessStep", "gmd")
                .putObjectTypeNamespaces("LI_Source", "gmd")
                .putObjectTypeNamespaces("CI_ResponsibleParty", "gmd")
                // valueWrap is recognised under either the alias path or the property-name path;
                // exercise both shapes against the same fixture.
                .putValueWrap("lebenszeitintervall", ["AA_Lebenszeitintervall", "beginnt"])
                .putValueWrap("qag.dpl.prs.des", ["AX_LI_ProcessStep_MitDatenerhebung_Description"])
                .putValueWrap("qag.dpl.prs.src.des", ["AX_Datenerhebung"])
                .codelistUriTemplate("https://registry.gdi-de.org/codelist/de.adv-online.gid/{{codelistId}}/{{value}}")
                .featureRefTemplate("urn:adv:oid:{{value}}")
                .build()
    }

    static FeatureTokenDecoderSimple<byte[], FeatureSchema, SchemaMapping, FeatureEventHandlerSimple.ModifiableContext<FeatureSchema, SchemaMapping>> newAxGebaeudeDecoder(
            FeatureSchema schema, FeatureTokenDecoderGmlInputProfile profile) {
        new FeatureTokenDecoderGml(
                AX_GEBAEUDE_NAMESPACES,
                [new QName(ADV_PREFIX_URI, "AX_Gebaeude")],
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
                profile)
    }

    def 'AX_Gebaeude: simple scalar property decodes via alias when wire prefix differs from configured prefix'() {
        given:
        def decoder = newAxGebaeudeDecoder(axGebaeudeSchema(), axGebaeudeProfile())
        // Wire uses prefix "adv" for the ADV URI; the decoder's namespace map only knows that URI
        // under prefix "aaa". The lookup must match on URI, not prefix.
        def xml = """<adv:AX_Gebaeude xmlns:adv="${ADV_PREFIX_URI}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000Egu">
              <adv:gebaeudefunktion>2500</adv:gebaeudefunktion>
            </adv:AX_Gebaeude>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["gfk"]) == "2500"
    }

    def 'AX_Gebaeude: valueWrap keyed by alias path decodes the wrapped scalar to the property source path'() {
        given:
        def decoder = newAxGebaeudeDecoder(axGebaeudeSchema(), axGebaeudeProfile())
        def xml = """<adv:AX_Gebaeude xmlns:adv="${ADV_PREFIX_URI}"
                xmlns:gml="http://www.opengis.net/gml/3.2"
                gml:id="DENW36AL10000Egu">
              <adv:lebenszeitintervall>
                <adv:AA_Lebenszeitintervall>
                  <adv:beginnt>2024-10-15T07:55:13Z</adv:beginnt>
                </adv:AA_Lebenszeitintervall>
              </adv:lebenszeitintervall>
            </adv:AX_Gebaeude>"""

        when:
        def tokens = runDecoder(decoder, xml)

        then:
        valueAtPath(tokens, ["lzi_beg"]) == "2024-10-15T07:55:13Z"
    }

    def 'AX_Gebaeude: full fixture decodes every property in AX_Gebaeude.xml to its SQL source path'() {
        given:
        def decoder = newAxGebaeudeDecoder(axGebaeudeSchema(), axGebaeudeProfile())
        def bytes = new File("src/test/resources/nas/AX_Gebaeude.xml").bytes

        when:
        def tokens = new ArrayList<>()
        def stream = Reactive.Source.inputStream(new ByteArrayInputStream(bytes))
                .via(decoder)
                .to(Reactive.Sink.reduce(tokens, (list, element) -> { list << element; return list }))
        stream.on(runner).run().toCompletableFuture().join()

        then:
        // gml:id → oid
        valueAtPath(tokens, ["oid"]) == "DENW36AL10000Egu"
        // gml:identifier (matched via the gml: prefix on the alias)
        valueAtPath(tokens, ["idn"]) == "urn:adv:oid:DENW36AL10000Egu"
        // lebenszeitintervall/AA_Lebenszeitintervall/beginnt → lzi_beg (alias-keyed valueWrap)
        valueAtPath(tokens, ["lzi_beg"]) == "2024-10-15T07:55:13Z"
        // gebaeudefunktion → gfk
        valueAtPath(tokens, ["gfk"]) == "2500"

        // modellart array: first member carries advStandardModell, second carries sonstigesModell
        // as xlink:href reduced through codelistUriTemplate.
        def matPath = ["mat"]
        def matObjectStarts = indicesOfTokenAtPath(tokens, FeatureTokenType.OBJECT, matPath)
        matObjectStarts.size() == 2
        def matValues = tokens.findAll { it instanceof String && (it == "DLKM" || it == "NWABK") }
        matValues == ["DLKM", "NWABK"]

        // anlass: VALUE_ARRAY of one codelist value reduced through codelistUriTemplate. The
        // xlink:title must not surface and the raw URI must not surface either.
        def anlArrayStart = indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY, ["anl"])
        def anlArrayEnd = indexOfTokenAtPath(tokens, FeatureTokenType.ARRAY_END, ["anl"])
        anlArrayStart >= 0 && anlArrayEnd > anlArrayStart
        def anlCodeIdx = tokens.indexOf("010704")
        anlCodeIdx > anlArrayStart && anlCodeIdx < anlArrayEnd
        !tokens.contains("https://registry.gdi-de.org/codelist/de.adv-online.gid/AA_Anlassart/010704")
        !tokens.contains("Qualitätssicherung und Datenpflege")

        // qualitaetsangaben deep tree: every leaf reaches its qag__* column source path.
        valueAtPath(tokens, ["qag", "dpl", "prs", "des"]) == "Erhebung"
        valueAtPath(tokens, ["qag", "dpl", "prs", "pro", "org"]) == "Amt für Bodenmanagement und Geoinformation Bonn"
        valueAtPath(tokens, ["qag", "dpl", "prs", "pro", "rol"]) == "processor"
        valueAtPath(tokens, ["qag", "dpl", "prs", "src", "des"]) == "1000"

        // hat: FEATURE_REF xlink:href reduced to the bare id via featureRefTemplate.
        valueAtPath(tokens, ["hat"]) == "DENW36AL10000K6p"
        !tokens.contains("urn:adv:oid:DENW36AL10000K6p")

        // position: a single CURVE_POLYGON-typed Geometry emitted at the gpo source path.
        def geometries = tokens.findAll { it instanceof Geometry } as List<Geometry>
        geometries.size() == 1
        pathBeforeGeometry(tokens) == ["gpo"]
    }
}
