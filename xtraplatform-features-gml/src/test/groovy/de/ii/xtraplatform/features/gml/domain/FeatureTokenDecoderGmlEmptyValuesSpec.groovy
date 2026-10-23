/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain

import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.features.domain.ImmutableFeatureQuery
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableSchemaMapping
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.streams.app.ReactiveRx
import de.ii.xtraplatform.streams.domain.Reactive
import spock.lang.Shared
import spock.lang.Specification

import javax.xml.namespace.QName

/**
 * A property element that is present but carries no value states an empty value, the way a JSON
 * member does with {@code ""}. The decoder used to drop it, which made the empty value of a scalar
 * property invisible; these specs pin the emission and the {@code rejectEmptyValues} check built on
 * it, together with the whitespace that separates child elements, which is not a value of its own.
 */
class FeatureTokenDecoderGmlEmptyValuesSpec extends Specification {

    static final String NS = "http://example.com/ns/1.0"
    static final Map<String, String> NAMESPACES = [
            "ex"   : NS,
            "gml"  : "http://www.opengis.net/gml/3.2",
            "xlink": "http://www.w3.org/1999/xlink",
            "xsi"  : "http://www.w3.org/2001/XMLSchema-instance"
    ]

    @Shared Reactive reactive
    @Shared Reactive.Runner runner

    def setupSpec() {
        reactive = new ReactiveRx()
        runner = reactive.runner("test-empty-values")
    }

    def cleanupSpec() {
        runner.close()
    }

    private FeatureTokenDecoderGml decoder(boolean rejectEmptyValues) {
        def schema = new ImmutableFeatureSchema.Builder()
                .name("party")
                .sourcePath("/party")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .sourcePath("objid")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID)
                        .alias("id"))
                .putProperties2("givenName", new ImmutableFeatureSchema.Builder()
                        .sourcePath("given_name")
                        .type(SchemaBase.Type.STRING)
                        .alias("givenName"))
                .putProperties2("note", new ImmutableFeatureSchema.Builder()
                        .sourcePath("note")
                        .type(SchemaBase.Type.STRING)
                        .alias("note"))
                .build()
        return new FeatureTokenDecoderGml(
                NAMESPACES,
                [new QName(NS, "Party")],
                schema,
                ImmutableFeatureQuery.builder().type(schema.getName()).build(),
                Map.of(schema.getName(), new ImmutableSchemaMapping.Builder()
                        .targetSchema(schema)
                        .sourcePathTransformer((path, isValue) -> path)
                        .build()),
                EpsgCrs.of(25832),
                Optional.empty(),
                Optional.empty(),
                ImmutableFeatureTokenDecoderGmlInputProfile.builder()
                        .useAlias(true)
                        .rejectEmptyValues(rejectEmptyValues)
                        .build())
    }

    private static String feature(String properties) {
        return '<ex:Party xmlns:ex="' + NS + '"' +
                ' xmlns:gml="http://www.opengis.net/gml/3.2"' +
                ' xmlns:xlink="http://www.w3.org/1999/xlink"' +
                ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' +
                ' gml:id="p1">' + properties + '</ex:Party>'
    }

    private List<Object> run(String properties, boolean rejectEmptyValues = false) {
        return Reactive.Source.inputStream(new ByteArrayInputStream(feature(properties).getBytes('UTF-8')))
                .via(decoder(rejectEmptyValues))
                .to(Reactive.Sink.reduce([], (list, element) -> { list << element; return list }))
                .on(runner).run().toCompletableFuture().join() as List<Object>
    }

    private static List<String> valueOf(List<Object> tokens, String property) {
        def values = []
        for (int i = 0; i < tokens.size() - 1; i++) {
            if (tokens[i] instanceof List && tokens[i] == [property]) {
                values << tokens[i + 1]
            }
        }
        return values
    }

    def 'a property element with no content states an empty value'() {
        when:
        def tokens = run('<ex:givenName></ex:givenName>')

        then:
        valueOf(tokens, 'givenName') == ['']
    }

    def 'a self-closing property element states an empty value'() {
        when:
        def tokens = run('<ex:givenName/>')

        then:
        valueOf(tokens, 'givenName') == ['']
    }

    def 'content of only whitespace states an empty value'() {
        when:
        def tokens = run('<ex:givenName>   </ex:givenName>')

        then: 'whitespace never reaches the character buffer, so it arrives as no content at all'
        valueOf(tokens, 'givenName') == ['']
    }

    def 'content is unaffected'() {
        when:
        def tokens = run('<ex:givenName>Alex</ex:givenName>')

        then:
        valueOf(tokens, 'givenName') == ['Alex']
    }

    def 'the whitespace that separates property elements is not a value'() {
        when:
        def tokens = run('\n  <ex:givenName>Alex</ex:givenName>\n  <ex:note>a note</ex:note>\n')

        then: 'the indentation between the properties reaches neither of them nor the feature'
        valueOf(tokens, 'givenName') == ['Alex']
        valueOf(tokens, 'note') == ['a note']
    }

    def 'an empty value is rejected under rejectEmptyValues'() {
        when:
        run('<ex:givenName></ex:givenName>', true)

        then:
        Throwable e = thrown()
        rootCause(e) instanceof IllegalArgumentException
        rootCause(e).message.contains("'givenName' has an empty value")
    }

    def 'the rejection is not dressed up as a parse failure'() {
        when:
        run('<ex:givenName></ex:givenName>', true)

        then: 'the document parsed; only the value was rejected, so the message says only that'
        Throwable e = thrown()
        def reported = e.cause ?: e
        reported.message.startsWith("The property 'givenName' has an empty value")
        !reported.message.contains('Could not parse GML')
    }

    def 'a self-closing property element is rejected under rejectEmptyValues'() {
        when:
        run('<ex:givenName/>', true)

        then:
        Throwable e = thrown()
        rootCause(e) instanceof IllegalArgumentException
    }

    def 'content of only whitespace is rejected under rejectEmptyValues'() {
        when:
        run('<ex:givenName>   </ex:givenName>', true)

        then:
        Throwable e = thrown()
        rootCause(e) instanceof IllegalArgumentException
    }

    def 'a property with content passes under rejectEmptyValues'() {
        when:
        def tokens = run('<ex:givenName>Alex</ex:givenName>', true)

        then:
        notThrown(Throwable)
        valueOf(tokens, 'givenName') == ['Alex']
    }

    def 'a reference carries its value in xlink:href and is not empty'() {
        when:
        def tokens = run('<ex:note xlink:href="https://example.com/notes/1"/>', true)

        then:
        notThrown(Throwable)
        valueOf(tokens, 'note') == ['https://example.com/notes/1']
    }

    def 'xsi:nil states the absence of a value, not an empty one'() {
        when:
        def tokens = run('<ex:note xsi:nil="true"/>', true)

        then: 'no value is decoded at all, so there is nothing to reject'
        notThrown(Throwable)
        valueOf(tokens, 'note') == []
    }

    private static Throwable rootCause(Throwable e) {
        Throwable cause = e
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause()
        }
        cause
    }
}
